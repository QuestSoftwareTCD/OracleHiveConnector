/**
 *   Copyright 2011 Quest Software, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.quest.orahive;

import java.io.InputStream;
import java.io.Reader;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.service.HiveInterface;
import org.apache.hadoop.io.BytesWritable;

/**
 * Wrapper around a HiveResultSet to fetch results in batches.
 * 
 * Pointless for versions after https://issues.apache.org/jira/browse/HIVE-1815 was fixed.
 * 
 * @author Peter Hall
 */
public class FetchNResultSet implements ResultSet
{
  private final ResultSet hiveResultSet; // Class unspecified as package changed between hive 0.5.0 and 0.7.0
  private final SerDe ds;
  private final HiveInterface hiveInterface;
  private int fetchSize = Constants.DEFAULT_ORACLE_INSERT_BATCH_SIZE;

  private List<String> currentBatch = Collections.emptyList();
  private Iterator<String> batchIter = currentBatch.iterator();
  private final List<Object> currentRow;
  private boolean use05deserialize;

  public FetchNResultSet(ResultSet hiveResultSet)
  {
    this.hiveResultSet = hiveResultSet;
    Field dsField = null;
    try
    {
      Field clientField = hiveResultSet.getClass().getDeclaredField("client");
      if (!clientField.isAccessible())
        clientField.setAccessible(true);
      hiveInterface = (HiveInterface)clientField.get(hiveResultSet);

      try
      {
        dsField = hiveResultSet.getClass().getDeclaredField("serde");
        use05deserialize = false;
      }
      catch(NoSuchFieldException nsfe)
      {
        dsField = hiveResultSet.getClass().getDeclaredField("ds");
        use05deserialize = true;
      }
      if (!dsField.isAccessible())
        dsField.setAccessible(true);
      ds = (SerDe)dsField.get(hiveResultSet);
      currentRow = new ArrayList<Object>(hiveResultSet.getMetaData().getColumnCount());
      for (int i = 0; i < hiveResultSet.getMetaData().getColumnCount(); i++)
      {
        currentRow.add(null);
      }
    }
    catch(Exception e)
    {
      throw new IllegalArgumentException("Unable to apply fetchN hack to hive driver", e);
    }
  }

  @SuppressWarnings({"unchecked", "cast"})
  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException
  {
    if (iface.isAssignableFrom(this.getClass()))
      return (T)this;
    if (iface.isAssignableFrom(hiveResultSet.getClass()))
      return (T)hiveResultSet;
    else
      return (T)hiveResultSet.unwrap(iface);
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException
  {
    if (iface.isAssignableFrom(this.getClass()) || iface.isAssignableFrom(hiveResultSet.getClass()))
    {
      return true;
    }
    return hiveResultSet.isWrapperFor(iface);
  }

  @Override
  public void setFetchSize(int rows) throws SQLException
  {
    fetchSize = rows;
  }

  @Override
  public int getFetchSize() throws SQLException
  {
    return fetchSize;
  }

  private void fetchNextBatch() throws SQLException
  {
    try
    {
      currentBatch = hiveInterface.fetchN(fetchSize);
      batchIter = currentBatch.iterator();
    }
    catch(Exception e)
    {
      throw new SQLException("Unable to fetch batch from Hive", e);
    }
  }

  @Override
  public boolean next() throws SQLException
  {
    if (!batchIter.hasNext())
    {
      fetchNextBatch();
    }

    if (batchIter.hasNext())
    {
      String data = batchIter.next();
      if (data.equals(""))
      {
        return false;
      }
      Object o;
      try
      {
        o = ds.deserialize(new BytesWritable(data.getBytes()));
      }
      catch(SerDeException e)
      {
        throw new SQLException("Unable to deserialize row from Hive", e);
      }

      if (use05deserialize)
      {
        hive05deserialize(o);
      }
      else
      {
        hive07deserialize(o);
      }

      return true;
    }
    else
    {
      return false;
    }
  }

  private void hive05deserialize(Object o)
  {
    ArrayList<?> row = (ArrayList<?>)o;
    for (int i = 0; i < row.size(); i++)
    {
      currentRow.set(i, row.get(i));
    }
  }

  private void hive07deserialize(Object o) throws SQLException
  {
    try
    {
      StructObjectInspector soi = (StructObjectInspector)ds.getObjectInspector();
      List<? extends StructField> fieldRefs = soi.getAllStructFieldRefs();
      for (int i = 0; i < fieldRefs.size(); i++)
      {
        StructField fieldRef = fieldRefs.get(i);
        ObjectInspector oi = fieldRef.getFieldObjectInspector();
        Object obj = soi.getStructFieldData(o, fieldRef);
        obj = ObjectInspectorUtils.copyToStandardObject(obj, oi, ObjectInspectorCopyOption.JAVA);
        if (obj != null && oi.getCategory() != ObjectInspector.Category.PRIMITIVE)
        {
          obj = obj.toString();
        }
        currentRow.set(i, obj);
      }
    }
    catch(SerDeException e)
    {
      throw new SQLException("Error fetching next row from hive", e);
    }
  }

  @Override
  public Object getObject(int columnIndex) throws SQLException
  {
    return currentRow.get(columnIndex - 1);
  }

  // Everything else passes args down to hiveResultSet

  @Override
  public boolean absolute(int row) throws SQLException
  {
    return hiveResultSet.absolute(row);
  }

  @Override
  public void afterLast() throws SQLException
  {
    hiveResultSet.afterLast();
  }

  @Override
  public void beforeFirst() throws SQLException
  {
    hiveResultSet.beforeFirst();
  }

  @Override
  public void cancelRowUpdates() throws SQLException
  {
    hiveResultSet.cancelRowUpdates();
  }

  @Override
  public void clearWarnings() throws SQLException
  {
    hiveResultSet.clearWarnings();
  }

  @Override
  public void close() throws SQLException
  {
    hiveResultSet.close();
  }

  @Override
  public void deleteRow() throws SQLException
  {
    hiveResultSet.deleteRow();
  }

  @Override
  public int findColumn(String columnName) throws SQLException
  {
    return hiveResultSet.findColumn(columnName);
  }

  @Override
  public boolean first() throws SQLException
  {
    return hiveResultSet.first();
  }

  @Override
  public Array getArray(int i) throws SQLException
  {
    return hiveResultSet.getArray(i);
  }

  @Override
  public Array getArray(String colName) throws SQLException
  {
    return hiveResultSet.getArray(colName);
  }

  @Override
  public InputStream getAsciiStream(int columnIndex) throws SQLException
  {
    return hiveResultSet.getAsciiStream(columnIndex);
  }

  @Override
  public InputStream getAsciiStream(String columnName) throws SQLException
  {
    return hiveResultSet.getAsciiStream(columnName);
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex) throws SQLException
  {
    return hiveResultSet.getBigDecimal(columnIndex);
  }

  @Override
  public BigDecimal getBigDecimal(String columnName) throws SQLException
  {
    return hiveResultSet.getBigDecimal(columnName);
  }

  @SuppressWarnings("deprecation")
  @Override
  public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException
  {
    return hiveResultSet.getBigDecimal(columnIndex, scale);
  }

  @SuppressWarnings("deprecation")
  @Override
  public BigDecimal getBigDecimal(String columnName, int scale) throws SQLException
  {
    return hiveResultSet.getBigDecimal(columnName, scale);
  }

  @Override
  public InputStream getBinaryStream(int columnIndex) throws SQLException
  {
    return hiveResultSet.getBinaryStream(columnIndex);
  }

  @Override
  public InputStream getBinaryStream(String columnName) throws SQLException
  {
    return hiveResultSet.getBinaryStream(columnName);
  }

  @Override
  public Blob getBlob(int i) throws SQLException
  {
    return hiveResultSet.getBlob(i);
  }

  @Override
  public Blob getBlob(String colName) throws SQLException
  {
    return hiveResultSet.getBlob(colName);
  }

  @Override
  public boolean getBoolean(int columnIndex) throws SQLException
  {
    return hiveResultSet.getBoolean(columnIndex);
  }

  @Override
  public boolean getBoolean(String columnName) throws SQLException
  {
    return hiveResultSet.getBoolean(columnName);
  }

  @Override
  public byte getByte(int columnIndex) throws SQLException
  {
    return hiveResultSet.getByte(columnIndex);
  }

  @Override
  public byte getByte(String columnName) throws SQLException
  {
    return hiveResultSet.getByte(columnName);
  }

  @Override
  public byte[] getBytes(int columnIndex) throws SQLException
  {
    return hiveResultSet.getBytes(columnIndex);
  }

  @Override
  public byte[] getBytes(String columnName) throws SQLException
  {
    return hiveResultSet.getBytes(columnName);
  }

  @Override
  public Reader getCharacterStream(int columnIndex) throws SQLException
  {
    return hiveResultSet.getCharacterStream(columnIndex);
  }

  @Override
  public Reader getCharacterStream(String columnName) throws SQLException
  {
    return hiveResultSet.getCharacterStream(columnName);
  }

  @Override
  public Clob getClob(int i) throws SQLException
  {
    return hiveResultSet.getClob(i);
  }

  @Override
  public Clob getClob(String colName) throws SQLException
  {
    return hiveResultSet.getClob(colName);
  }

  @Override
  public int getConcurrency() throws SQLException
  {
    return hiveResultSet.getConcurrency();
  }

  @Override
  public String getCursorName() throws SQLException
  {
    return hiveResultSet.getCursorName();
  }

  @Override
  public Date getDate(int columnIndex) throws SQLException
  {
    return hiveResultSet.getDate(columnIndex);
  }

  @Override
  public Date getDate(String columnName) throws SQLException
  {
    return hiveResultSet.getDate(columnName);
  }

  @Override
  public Date getDate(int columnIndex, Calendar cal) throws SQLException
  {
    return hiveResultSet.getDate(columnIndex, cal);
  }

  @Override
  public Date getDate(String columnName, Calendar cal) throws SQLException
  {
    return hiveResultSet.getDate(columnName, cal);
  }

  @Override
  public double getDouble(int columnIndex) throws SQLException
  {
    return hiveResultSet.getDouble(columnIndex);
  }

  @Override
  public double getDouble(String columnName) throws SQLException
  {
    return hiveResultSet.getDouble(columnName);
  }

  @Override
  public int getFetchDirection() throws SQLException
  {
    return hiveResultSet.getFetchDirection();
  }

  @Override
  public float getFloat(int columnIndex) throws SQLException
  {
    return hiveResultSet.getFloat(columnIndex);
  }

  @Override
  public float getFloat(String columnName) throws SQLException
  {
    return hiveResultSet.getFloat(columnName);
  }

  @Override
  public int getHoldability() throws SQLException
  {
    return hiveResultSet.getHoldability();
  }

  @Override
  public int getInt(int columnIndex) throws SQLException
  {
    return hiveResultSet.getInt(columnIndex);
  }

  @Override
  public int getInt(String columnName) throws SQLException
  {
    return hiveResultSet.getInt(columnName);
  }

  @Override
  public long getLong(int columnIndex) throws SQLException
  {
    return hiveResultSet.getLong(columnIndex);
  }

  @Override
  public long getLong(String columnName) throws SQLException
  {
    return hiveResultSet.getLong(columnName);
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException
  {
    return hiveResultSet.getMetaData();
  }

  @Override
  public Reader getNCharacterStream(int arg0) throws SQLException
  {
    return hiveResultSet.getNCharacterStream(arg0);
  }

  @Override
  public Reader getNCharacterStream(String arg0) throws SQLException
  {
    return hiveResultSet.getNCharacterStream(arg0);
  }

  @Override
  public NClob getNClob(int arg0) throws SQLException
  {
    return hiveResultSet.getNClob(arg0);
  }

  @Override
  public NClob getNClob(String columnLabel) throws SQLException
  {
    return hiveResultSet.getNClob(columnLabel);
  }

  @Override
  public String getNString(int columnIndex) throws SQLException
  {
    return hiveResultSet.getNString(columnIndex);
  }

  @Override
  public String getNString(String columnLabel) throws SQLException
  {
    return hiveResultSet.getNString(columnLabel);
  }

  @Override
  public Object getObject(String columnName) throws SQLException
  {
    return hiveResultSet.getObject(columnName);
  }

  @Override
  public Object getObject(int i, Map<String,Class<?>> map) throws SQLException
  {
    return hiveResultSet.getObject(i, map);
  }

  @Override
  public Object getObject(String colName, Map<String,Class<?>> map) throws SQLException
  {
    return hiveResultSet.getObject(colName, map);
  }

  @Override
  public Ref getRef(int i) throws SQLException
  {
    return hiveResultSet.getRef(i);
  }

  @Override
  public Ref getRef(String colName) throws SQLException
  {
    return hiveResultSet.getRef(colName);
  }

  @Override
  public int getRow() throws SQLException
  {
    return hiveResultSet.getRow();
  }

  @Override
  public RowId getRowId(int columnIndex) throws SQLException
  {
    return hiveResultSet.getRowId(columnIndex);
  }

  @Override
  public RowId getRowId(String columnLabel) throws SQLException
  {
    return hiveResultSet.getRowId(columnLabel);
  }

  @Override
  public SQLXML getSQLXML(int columnIndex) throws SQLException
  {
    return hiveResultSet.getSQLXML(columnIndex);
  }

  @Override
  public SQLXML getSQLXML(String columnLabel) throws SQLException
  {
    return hiveResultSet.getSQLXML(columnLabel);
  }

  @Override
  public short getShort(int columnIndex) throws SQLException
  {
    return hiveResultSet.getShort(columnIndex);
  }

  @Override
  public short getShort(String columnName) throws SQLException
  {
    return hiveResultSet.getShort(columnName);
  }

  @Override
  public Statement getStatement() throws SQLException
  {
    return hiveResultSet.getStatement();
  }

  @Override
  public String getString(int columnIndex) throws SQLException
  {
    return hiveResultSet.getString(columnIndex);
  }

  @Override
  public String getString(String columnName) throws SQLException
  {
    return hiveResultSet.getString(columnName);
  }

  @Override
  public Time getTime(int columnIndex) throws SQLException
  {
    return hiveResultSet.getTime(columnIndex);
  }

  @Override
  public Time getTime(String columnName) throws SQLException
  {
    return hiveResultSet.getTime(columnName);
  }

  @Override
  public Time getTime(int columnIndex, Calendar cal) throws SQLException
  {
    return hiveResultSet.getTime(columnIndex, cal);
  }

  @Override
  public Time getTime(String columnName, Calendar cal) throws SQLException
  {
    return hiveResultSet.getTime(columnName, cal);
  }

  @Override
  public Timestamp getTimestamp(int columnIndex) throws SQLException
  {
    return hiveResultSet.getTimestamp(columnIndex);
  }

  @Override
  public Timestamp getTimestamp(String columnName) throws SQLException
  {
    return hiveResultSet.getTimestamp(columnName);
  }

  @Override
  public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException
  {
    return hiveResultSet.getTimestamp(columnIndex, cal);
  }

  @Override
  public Timestamp getTimestamp(String columnName, Calendar cal) throws SQLException
  {
    return hiveResultSet.getTimestamp(columnName, cal);
  }

  @Override
  public int getType() throws SQLException
  {
    return hiveResultSet.getType();
  }

  @Override
  public URL getURL(int columnIndex) throws SQLException
  {
    return hiveResultSet.getURL(columnIndex);
  }

  @Override
  public URL getURL(String columnName) throws SQLException
  {
    return hiveResultSet.getURL(columnName);
  }

  @SuppressWarnings("deprecation")
  @Override
  public InputStream getUnicodeStream(int columnIndex) throws SQLException
  {
    return hiveResultSet.getUnicodeStream(columnIndex);
  }

  @SuppressWarnings("deprecation")
  @Override
  public InputStream getUnicodeStream(String columnName) throws SQLException
  {
    return hiveResultSet.getUnicodeStream(columnName);
  }

  @Override
  public SQLWarning getWarnings() throws SQLException
  {
    return hiveResultSet.getWarnings();
  }

  @Override
  public void insertRow() throws SQLException
  {
    hiveResultSet.insertRow();
  }

  @Override
  public boolean isAfterLast() throws SQLException
  {
    return hiveResultSet.isAfterLast();
  }

  @Override
  public boolean isBeforeFirst() throws SQLException
  {
    return hiveResultSet.isBeforeFirst();
  }

  @Override
  public boolean isClosed() throws SQLException
  {
    return hiveResultSet.isClosed();
  }

  @Override
  public boolean isFirst() throws SQLException
  {
    return hiveResultSet.isFirst();
  }

  @Override
  public boolean isLast() throws SQLException
  {
    return hiveResultSet.isLast();
  }

  @Override
  public boolean last() throws SQLException
  {
    return hiveResultSet.last();
  }

  @Override
  public void moveToCurrentRow() throws SQLException
  {
    hiveResultSet.moveToCurrentRow();
  }

  @Override
  public void moveToInsertRow() throws SQLException
  {
    hiveResultSet.moveToInsertRow();
  }

  @Override
  public boolean previous() throws SQLException
  {
    return hiveResultSet.previous();
  }

  @Override
  public void refreshRow() throws SQLException
  {
    hiveResultSet.refreshRow();
  }

  @Override
  public boolean relative(int rows) throws SQLException
  {
    return hiveResultSet.relative(rows);
  }

  @Override
  public boolean rowDeleted() throws SQLException
  {
    return hiveResultSet.rowDeleted();
  }

  @Override
  public boolean rowInserted() throws SQLException
  {
    return hiveResultSet.rowInserted();
  }

  @Override
  public boolean rowUpdated() throws SQLException
  {
    return hiveResultSet.rowUpdated();
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException
  {
    hiveResultSet.setFetchDirection(direction);
  }

  @Override
  public void updateArray(int columnIndex, Array x) throws SQLException
  {
    hiveResultSet.updateArray(columnIndex, x);
  }

  @Override
  public void updateArray(String columnName, Array x) throws SQLException
  {
    hiveResultSet.updateArray(columnName, x);
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException
  {
    hiveResultSet.updateAsciiStream(columnIndex, x);
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException
  {
    hiveResultSet.updateAsciiStream(columnLabel, x);
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException
  {
    hiveResultSet.updateAsciiStream(columnIndex, x, length);
  }

  @Override
  public void updateAsciiStream(String columnName, InputStream x, int length) throws SQLException
  {
    hiveResultSet.updateAsciiStream(columnName, x, length);
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException
  {
    hiveResultSet.updateAsciiStream(columnIndex, x, length);
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException
  {
    hiveResultSet.updateAsciiStream(columnLabel, x, length);
  }

  @Override
  public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException
  {
    hiveResultSet.updateBigDecimal(columnIndex, x);
  }

  @Override
  public void updateBigDecimal(String columnName, BigDecimal x) throws SQLException
  {
    hiveResultSet.updateBigDecimal(columnName, x);
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException
  {
    hiveResultSet.updateBinaryStream(columnIndex, x);
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException
  {
    hiveResultSet.updateBinaryStream(columnLabel, x);
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException
  {
    hiveResultSet.updateBinaryStream(columnIndex, x, length);
  }

  @Override
  public void updateBinaryStream(String columnName, InputStream x, int length) throws SQLException
  {
    hiveResultSet.updateBinaryStream(columnName, x, length);
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException
  {
    hiveResultSet.updateBinaryStream(columnIndex, x, length);
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException
  {
    hiveResultSet.updateBinaryStream(columnLabel, x, length);
  }

  @Override
  public void updateBlob(int columnIndex, Blob x) throws SQLException
  {
    hiveResultSet.updateBlob(columnIndex, x);
  }

  @Override
  public void updateBlob(String columnName, Blob x) throws SQLException
  {
    hiveResultSet.updateBlob(columnName, x);
  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException
  {
    hiveResultSet.updateBlob(columnIndex, inputStream);
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException
  {
    hiveResultSet.updateBlob(columnLabel, inputStream);
  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException
  {
    hiveResultSet.updateBlob(columnIndex, inputStream, length);
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException
  {
    hiveResultSet.updateBlob(columnLabel, inputStream, length);
  }

  @Override
  public void updateBoolean(int columnIndex, boolean x) throws SQLException
  {
    hiveResultSet.updateBoolean(columnIndex, x);
  }

  @Override
  public void updateBoolean(String columnName, boolean x) throws SQLException
  {
    hiveResultSet.updateBoolean(columnName, x);
  }

  @Override
  public void updateByte(int columnIndex, byte x) throws SQLException
  {
    hiveResultSet.updateByte(columnIndex, x);
  }

  @Override
  public void updateByte(String columnName, byte x) throws SQLException
  {
    hiveResultSet.updateByte(columnName, x);
  }

  @Override
  public void updateBytes(int columnIndex, byte[] x) throws SQLException
  {
    hiveResultSet.updateBytes(columnIndex, x);
  }

  @Override
  public void updateBytes(String columnName, byte[] x) throws SQLException
  {
    hiveResultSet.updateBytes(columnName, x);
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x) throws SQLException
  {
    hiveResultSet.updateCharacterStream(columnIndex, x);
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException
  {
    hiveResultSet.updateCharacterStream(columnLabel, reader);
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException
  {
    hiveResultSet.updateCharacterStream(columnIndex, x, length);
  }

  @Override
  public void updateCharacterStream(String columnName, Reader reader, int length) throws SQLException
  {
    hiveResultSet.updateCharacterStream(columnName, reader, length);
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException
  {
    hiveResultSet.updateCharacterStream(columnIndex, x, length);
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException
  {
    hiveResultSet.updateCharacterStream(columnLabel, reader, length);
  }

  @Override
  public void updateClob(int columnIndex, Clob x) throws SQLException
  {
    hiveResultSet.updateClob(columnIndex, x);
  }

  @Override
  public void updateClob(String columnName, Clob x) throws SQLException
  {
    hiveResultSet.updateClob(columnName, x);
  }

  @Override
  public void updateClob(int columnIndex, Reader reader) throws SQLException
  {
    hiveResultSet.updateClob(columnIndex, reader);
  }

  @Override
  public void updateClob(String columnLabel, Reader reader) throws SQLException
  {
    hiveResultSet.updateClob(columnLabel, reader);
  }

  @Override
  public void updateClob(int columnIndex, Reader reader, long length) throws SQLException
  {
    hiveResultSet.updateClob(columnIndex, reader, length);
  }

  @Override
  public void updateClob(String columnLabel, Reader reader, long length) throws SQLException
  {
    hiveResultSet.updateClob(columnLabel, reader, length);
  }

  @Override
  public void updateDate(int columnIndex, Date x) throws SQLException
  {
    hiveResultSet.updateDate(columnIndex, x);
  }

  @Override
  public void updateDate(String columnName, Date x) throws SQLException
  {
    hiveResultSet.updateDate(columnName, x);
  }

  @Override
  public void updateDouble(int columnIndex, double x) throws SQLException
  {
    hiveResultSet.updateDouble(columnIndex, x);
  }

  @Override
  public void updateDouble(String columnName, double x) throws SQLException
  {
    hiveResultSet.updateDouble(columnName, x);
  }

  @Override
  public void updateFloat(int columnIndex, float x) throws SQLException
  {
    hiveResultSet.updateFloat(columnIndex, x);
  }

  @Override
  public void updateFloat(String columnName, float x) throws SQLException
  {
    hiveResultSet.updateFloat(columnName, x);
  }

  @Override
  public void updateInt(int columnIndex, int x) throws SQLException
  {
    hiveResultSet.updateInt(columnIndex, x);
  }

  @Override
  public void updateInt(String columnName, int x) throws SQLException
  {
    hiveResultSet.updateInt(columnName, x);
  }

  @Override
  public void updateLong(int columnIndex, long x) throws SQLException
  {
    hiveResultSet.updateLong(columnIndex, x);
  }

  @Override
  public void updateLong(String columnName, long x) throws SQLException
  {
    hiveResultSet.updateLong(columnName, x);
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException
  {
    hiveResultSet.updateNCharacterStream(columnIndex, x);
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException
  {
    hiveResultSet.updateNCharacterStream(columnLabel, reader);
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException
  {
    hiveResultSet.updateNCharacterStream(columnIndex, x, length);
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException
  {
    hiveResultSet.updateNCharacterStream(columnLabel, reader, length);
  }

  @Override
  public void updateNClob(int columnIndex, NClob clob) throws SQLException
  {
    hiveResultSet.updateNClob(columnIndex, clob);
  }

  @Override
  public void updateNClob(String columnLabel, NClob clob) throws SQLException
  {
    hiveResultSet.updateNClob(columnLabel, clob);
  }

  @Override
  public void updateNClob(int columnIndex, Reader reader) throws SQLException
  {
    hiveResultSet.updateNClob(columnIndex, reader);
  }

  @Override
  public void updateNClob(String columnLabel, Reader reader) throws SQLException
  {
    hiveResultSet.updateNClob(columnLabel, reader);
  }

  @Override
  public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException
  {
    hiveResultSet.updateNClob(columnIndex, reader, length);
  }

  @Override
  public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException
  {
    hiveResultSet.updateNClob(columnLabel, reader, length);
  }

  @Override
  public void updateNString(int columnIndex, String string) throws SQLException
  {
    hiveResultSet.updateNString(columnIndex, string);
  }

  @Override
  public void updateNString(String columnLabel, String string) throws SQLException
  {
    hiveResultSet.updateNString(columnLabel, string);
  }

  @Override
  public void updateNull(int columnIndex) throws SQLException
  {
    hiveResultSet.updateNull(columnIndex);
  }

  @Override
  public void updateNull(String columnName) throws SQLException
  {
    hiveResultSet.updateNull(columnName);
  }

  @Override
  public void updateObject(int columnIndex, Object x) throws SQLException
  {
    hiveResultSet.updateObject(columnIndex, x);
  }

  @Override
  public void updateObject(String columnName, Object x) throws SQLException
  {
    hiveResultSet.updateObject(columnName, x);
  }

  @Override
  public void updateObject(int columnIndex, Object x, int scale) throws SQLException
  {
    hiveResultSet.updateObject(columnIndex, x, scale);
  }

  @Override
  public void updateObject(String columnName, Object x, int scale) throws SQLException
  {
    hiveResultSet.updateObject(columnName, x, scale);
  }

  @Override
  public void updateRef(int columnIndex, Ref x) throws SQLException
  {
    hiveResultSet.updateRef(columnIndex, x);
  }

  @Override
  public void updateRef(String columnName, Ref x) throws SQLException
  {
    hiveResultSet.updateRef(columnName, x);
  }

  @Override
  public void updateRow() throws SQLException
  {
    hiveResultSet.updateRow();
  }

  @Override
  public void updateRowId(int columnIndex, RowId x) throws SQLException
  {
    hiveResultSet.updateRowId(columnIndex, x);
  }

  @Override
  public void updateRowId(String columnLabel, RowId x) throws SQLException
  {
    hiveResultSet.updateRowId(columnLabel, x);
  }

  @Override
  public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException
  {
    hiveResultSet.updateSQLXML(columnIndex, xmlObject);
  }

  @Override
  public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException
  {
    hiveResultSet.updateSQLXML(columnLabel, xmlObject);
  }

  @Override
  public void updateShort(int columnIndex, short x) throws SQLException
  {
    hiveResultSet.updateShort(columnIndex, x);
  }

  @Override
  public void updateShort(String columnName, short x) throws SQLException
  {
    hiveResultSet.updateShort(columnName, x);
  }

  @Override
  public void updateString(int columnIndex, String x) throws SQLException
  {
    hiveResultSet.updateString(columnIndex, x);
  }

  @Override
  public void updateString(String columnName, String x) throws SQLException
  {
    hiveResultSet.updateString(columnName, x);
  }

  @Override
  public void updateTime(int columnIndex, Time x) throws SQLException
  {
    hiveResultSet.updateTime(columnIndex, x);
  }

  @Override
  public void updateTime(String columnName, Time x) throws SQLException
  {
    hiveResultSet.updateTime(columnName, x);
  }

  @Override
  public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException
  {
    hiveResultSet.updateTimestamp(columnIndex, x);
  }

  @Override
  public void updateTimestamp(String columnName, Timestamp x) throws SQLException
  {
    hiveResultSet.updateTimestamp(columnName, x);
  }

  @Override
  public boolean wasNull() throws SQLException
  {
    return hiveResultSet.wasNull();
  }

}
