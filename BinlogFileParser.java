package com.lt.tool;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BinlogFileParse {

	/** 总事件类型个数38 */
	public static final int EVENT_TYPE_COUNT = 38;
	public static final int START_EVENT_V3 = 1;
	public static final int QUERY_EVENT = 2;
	public static final int STOP_EVENT = 3;
	public static final int ROTATE_EVENT = 4;
	public static final int INTVAR_EVENT = 5;
	public static final int LOAD_EVENT = 6;
	public static final int SLAVE_EVENT = 7;
	public static final int CREATE_FILE_EVENT = 8;
	public static final int APPEND_BLOCK_EVENT = 9;
	public static final int EXEC_LOAD_EVENT = 10;
	public static final int DELETE_FILE_EVENT = 11;
	public static final int NEW_LOAD_EVENT = 12;
	public static final int RAND_EVENT = 13;
	public static final int USER_VAR_EVENT = 14;
	public static final int FORMAT_DESCRIPTION_EVENT = 15;
	public static final int XID_EVENT = 16;
	public static final int BEGIN_LOAD_QUERY_EVENT = 17;
	public static final int EXECUTE_LOAD_QUERY_EVENT = 18;
	public static final int TABLE_MAP_EVENT = 19;
	public static final int PRE_GA_WRITE_ROWS_EVENT = 20;
	public static final int PRE_GA_UPDATE_ROWS_EVENT = 21;
	public static final int PRE_GA_DELETE_ROWS_EVENT = 22;
	public static final int WRITE_ROWS_EVENT_V1 = 23;
	public static final int UPDATE_ROWS_EVENT_V1 = 24;
	public static final int DELETE_ROWS_EVENT_V1 = 25;
	public static final int INCIDENT_EVENT = 26;
	public static final int HEARTBEAT_LOG_EVENT = 27;
	public static final int IGNORABLE_LOG_EVENT = 28;
	public static final int ROWS_QUERY_LOG_EVENT = 29;
	public static final int WRITE_ROWS_EVENT = 30;
	public static final int UPDATE_ROWS_EVENT = 31;
	public static final int DELETE_ROWS_EVENT = 32;
	public static final int GTID_LOG_EVENT = 33;
	public static final int ANONYMOUS_GTID_LOG_EVENT = 34;
	public static final int PREVIOUS_GTIDS_LOG_EVENT = 35;
	public static final int TRANSACTION_CONTEXT_EVENT = 36;
	public static final int VIEW_CHANGE_EVENT = 37;
	public static final int XA_PREPARE_LOG_EVENT = 38;

	// mysql的数据类型
	public static final int MYSQL_TYPE_DECIMAL = 0;
	public static final int MYSQL_TYPE_TINY = 1;
	public static final int MYSQL_TYPE_SHORT = 2;
	public static final int MYSQL_TYPE_LONG = 3;
	public static final int MYSQL_TYPE_FLOAT = 4;
	public static final int MYSQL_TYPE_DOUBLE = 5;
	public static final int MYSQL_TYPE_NULL = 6;
	public static final int MYSQL_TYPE_TIMESTAMP = 7;
	public static final int MYSQL_TYPE_LONGLONG = 8;
	public static final int MYSQL_TYPE_INT24 = 9;
	public static final int MYSQL_TYPE_DATE = 10;
	public static final int MYSQL_TYPE_TIME = 11;
	public static final int MYSQL_TYPE_DATETIME = 12;
	public static final int MYSQL_TYPE_YEAR = 13;
	public static final int MYSQL_TYPE_NEWDATE = 14;
	public static final int MYSQL_TYPE_VARCHAR = 15;
	public static final int MYSQL_TYPE_BIT = 16;
	public static final int MYSQL_TYPE_TIMESTAMP2 = 17;
	public static final int MYSQL_TYPE_DATETIME2 = 18;
	public static final int MYSQL_TYPE_TIME2 = 19;
	public static final int MYSQL_TYPE_NEWDECIMAL = 246;
	public static final int MYSQL_TYPE_ENUM = 247;
	public static final int MYSQL_TYPE_SET = 248;
	public static final int MYSQL_TYPE_TINY_BLOB = 249;
	public static final int MYSQL_TYPE_MEDIUM_BLOB = 250;
	public static final int MYSQL_TYPE_LONG_BLOB = 251;
	public static final int MYSQL_TYPE_BLOB = 252;
	public static final int MYSQL_TYPE_VAR_STRING = 253;
	public static final int MYSQL_TYPE_STRING = 254;
	public static final int MYSQL_TYPE_GEOMETRY = 255;

	// 每9位数字占用4个字节，其他数字占用字节数从数据中获取
	private static final int DIGITS_PER_4BYTES = 9;
	private static final BigDecimal POSITIVE_ONE = BigDecimal.ONE;
	private static final BigDecimal NEGATIVE_ONE = new BigDecimal("-1");
	private static final int DECIMAL_BINARY_SIZE[] = { 0, 1, 1, 2, 2, 3, 3, 4, 4, 4 };

	// 16进制
	public static final int HEX = 16;

	// param
	public static String driver = "com.mysql.jdbc.Driver";
	public static String url;
	public static String user;
	public static String password;
	public static boolean generateReverseSql;
	public static boolean simple;
	public static boolean onlychange;

	// read file
	private static byte[] readFile(String file) {
		byte bt[] = null;

		InputStream is = null;
		try {
			is = new FileInputStream(file);
			int length = is.available();
			bt = new byte[length];
			is.read(bt);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (null != is) {
				try {
					is.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		return bt;
	}

	public static void parse(byte[] bt) {

		CodeStream cs = new CodeStream(bt);
				// get header
		if (!"fe62696e".equals(cs.nextHexString(4))) {
			System.out.println("not a binary log,error!");
			return;
		}

		// 第一个event为FORMAT_DESCRIPTION_EVENT，长度为19（header） + 95（fix data）+5（Variable data）
		// header(19)[timestamp(4),type_code(1),server_id(4),event_length(4),next_position(4),flags(2)]
		Header begin = new Header(cs.sub(19));

		// fix data(95)[binlog_version(2),server_version(50),create_timestamp(4),header_length(1),post-header(38)]
		cs.getAndIncrement(56);
		final int hl = cs.nextInt(1);
		Map<Integer, Integer> fixLen = new HashMap<Integer, Integer>();
		for (int i = 1; i <= EVENT_TYPE_COUNT; i++) {
			fixLen.put(i, cs.nextInt(1));
		}

		// 第1个event过滤后的长度,重置cs的pos位置
		cs.setPos(begin.nextPosition);

		// 用来存放binlog中适配出来的表map信息
		Map<Integer, Table> tables = new HashMap<Integer, Table>();

		while (cs.getPos() < bt.length) {
			Header h = new Header(cs.sub(hl));
			// 获取fix data length
			int fl = fixLen.get(h.getTypeCode());
			// byte[] cur_event = cs.sub(h.next_position - cs.getPos());
			CodeStream scs = cs.subCS(h.nextPosition - cs.getPos());

			switch (h.typeCode) {
			case UPDATE_ROWS_EVENT:
			case DELETE_ROWS_EVENT:
			case WRITE_ROWS_EVENT:
				List<Record> rs = parseDmlRows(scs, tables, fl, h.typeCode);
				for (Record r : rs) {
					System.out.println(r);
				}
				// System.out.println(parseDmlRows(scs, tables, fl, h.type_code));
				break;
			case TABLE_MAP_EVENT:
				parseTableMap(scs, tables, fl);
				break;
			default:
				break;
			}
		}
	}

	private static List<Record> parseDmlRows(CodeStream cs, Map<Integer, Table> tables, int fl, int et) {

		List<Record> rs = new ArrayList<Record>();
		// fix data:6 bytes. The table ID
		int tbid = cs.nextInt(6, true);
		Table table = tables.get(tbid);

		if (null == table) {
			return null;
		}

		cs.getAndIncrement(fl - 6);

		int cm = cs.nextInt(1);
		int bitLen = (int) (cm + 7) / 8;

		char[] cons = cs.nextCharArray(bitLen);
		char[] consu = null;

		// update使用
		if (et == UPDATE_ROWS_EVENT) {
			consu = cs.nextCharArray(bitLen);
		}

		int cl = cs.getBt().length;
		while (cs.getPos() < cl - 4) {
			Record r = new Record();
			r.setGenerateReverseSql(generateReverseSql);
			r.setEventType(et);
			r.setTbname(table.getDbname() + "." + table.getTbname());
			r.setPrimaryKey(table.primarykey);

			char[] nulls = cs.nextCharArray(bitLen);

			r.setVmap(buildColumn(cs, table, cm, cons, nulls));

			if (et == UPDATE_ROWS_EVENT) {
				r.setVmapUpdate(buildColumn(cs, table, cm, consu, cs.nextCharArray(bitLen)));
			}

			rs.add(r);
		}

		return rs;
	}

	// table map event
	private static void parseTableMap(CodeStream cs, Map<Integer, Table> tables, int fl) {
		// fix data:6 bytes. The table ID
		int tableid = cs.nextInt(6, true);
		cs.getAndIncrement(fl - 6);
		// 如果已经解析过表则直接跳过，不再解析
		if (tables.containsKey(tableid)) {
			return;
		}

		Table tb = new Table();
		// 1 byte. The length of the database name.
		String dbname = cs.nextCharsetString(cs.nextInt(1) + 1).trim();
		tb.setDbname(dbname);
		String tbname = cs.nextCharsetString(cs.nextInt(1) + 1).trim();
		tb.setTbname(tbname);
		long colNum = cs.nextPackedLong();
		boolean primarykey = false;
		Map<Integer, Column> cols = getMds(tbname, dbname);

		if (null == cols || cols.isEmpty()) {
			System.out.println("get mds fail,table is " + dbname + "." + tbname);
			return;
		}
		// get col type
		for (int k = 1; k <= colNum; k++) {
			Column col = cols.get(k);
			if (col.primaryKey) {
				primarykey = true;
			}
			col.setColType(cs.nextInt(1));
			cols.put(k, col);
		}

		// meta data长度，暂未使用
		cs.nextInt(1);

		for (int k = 1; k <= colNum; k++) {
			Column col = cols.get(k);
			col.setMetaData(cs);
		}
		
		tb.setPrimarykey(primarykey);
		tb.setCols(cols);
		tables.put(tableid, tb);
	}

	private static Map<Integer, Column> buildColumn(CodeStream cs, Table table, int cm, char[] cons, char[] nulls) {
		Map<Integer, Column> cols = table.cloneCols();

		for (int i = 1; i <= cm; i++) {

			Column column = cols.get(i);

			column.setContain('1' == cons[cons.length - i] ? true : false);
			column.setEmpty('1' == nulls[nulls.length - i] ? true : false);

			if (column.isContain()) {
				final int meta = column.getMetaData();
				int type = column.getColType();
				if (type == MYSQL_TYPE_STRING && meta > 256) {
					final int meta0 = meta >> 8;
					final int meta1 = meta & 0xFF;
					if ((meta0 & 0x30) != 0x30) { // a long CHAR() field: see #37426
						column.setColType(meta0 | 0x30);
						column.setLength(meta1 | (((meta0 & 0x30) ^ 0x30) << 4));
					} else {
						switch (meta0) {
						case MYSQL_TYPE_SET:
						case MYSQL_TYPE_ENUM:
						case MYSQL_TYPE_STRING:
							column.setColType(meta0);
							column.setLength(meta1);
							break;
						default:
							throw new RuntimeException("assertion failed, unknown column type: " + type);
						}
					}
				}

				if (!column.empty) {
					column.setValue(cs.nextVal(column));
				}
			}
		}

		return cols;
	}

	private static Map<Integer, Column> getMds(String tbname, String dbname) {
		// String driver = "com.mysql.jdbc.Driver";
		try {
			Class.forName(driver);
		} catch (ClassNotFoundException e) {
			System.out.println(e.getMessage());
		}
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;

		Map<Integer, Column> cols = new HashMap<Integer, Column>();

		try {
			conn = DriverManager.getConnection(url, user, password);
			ps = conn.prepareStatement(
					"select column_name,ordinal_position,column_key from information_schema.columns where table_name=? and table_schema=?");
			ps.setString(1, tbname);
			ps.setString(2, dbname);
			rs = ps.executeQuery();

			while (rs.next()) {
				Column col = new Column();
				int index;
				col.setColindex(index = rs.getInt("ordinal_position"));
				col.setColname(rs.getString("column_name"));
				col.setPrimaryKey("PRI".equals(rs.getString("column_key")));
				cols.put(index, col);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			close(rs, ps, conn);
		}

		return cols;
	}

	// release resource
	private static void close(AutoCloseable... closes) {
		try {
			for (AutoCloseable close : closes) {
				if (null != close) {
					close.close();
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// header class
	static class Header {

		private int typeCode;
		private int serverId;
		private int eventLength;
		private int nextPosition;
		private String extraHeaders;

		public Header(byte[] bt) {
			CodeStream cs = new CodeStream(bt);
			cs.getAndIncrement(4);
			this.typeCode = cs.nextInt(1);
			this.serverId = cs.nextInt(4);
			this.eventLength = cs.nextInt(4, true);
			this.nextPosition = cs.nextInt(4, true);
		}

		public int getTypeCode() {
			return typeCode;
		}

		public void setTypeCode(int typeCode) {
			this.typeCode = typeCode;
		}

		public int getServerId() {
			return serverId;
		}

		public void setServerId(int serverId) {
			this.serverId = serverId;
		}

		public int getEventLength() {
			return eventLength;
		}

		public void setEventLength(int eventLength) {
			this.eventLength = eventLength;
		}

		public int getNextPosition() {
			return nextPosition;
		}

		public void setNextPosition(int nextPosition) {
			this.nextPosition = nextPosition;
		}

		public String getExtraHeaders() {
			return extraHeaders;
		}

		public void setExtraHeaders(String extraHeaders) {
			this.extraHeaders = extraHeaders;
		}

		@Override
		public String toString() {
			return "Event Type:" + this.typeCode + " Server id:" + this.serverId + " Event len:" + this.eventLength + " Next Pos:"
					+ this.nextPosition;
		}
	}

	// table class
	static class Table {

		private String tbname;

		private int tbid;

		private String dbname;

		private boolean primarykey;

		private Map<Integer, Column> cols = new HashMap<Integer, Column>();

		public String getTbname() {
			return tbname;
		}

		public void setTbname(String tbname) {
			this.tbname = tbname;
		}

		public int getTbid() {
			return tbid;
		}

		public void setTbid(int tbid) {
			this.tbid = tbid;
		}

		public Map<Integer, Column> getCols() {
			return cols;
		}

		public void setCols(Map<Integer, Column> cols) {
			this.cols = cols;
		}

		public String getDbname() {
			return dbname;
		}

		public void setDbname(String dbname) {
			this.dbname = dbname;
		}

		public boolean isPrimarykey() {
			return primarykey;
		}

		public void setPrimarykey(boolean primarykey) {
			this.primarykey = primarykey;
		}

		@SuppressWarnings("unchecked")
		public Map<Integer, Column> cloneCols() {
			Object clone = null;

			try {
				if (cols != null) {
					ByteArrayOutputStream baos = new ByteArrayOutputStream();
					ObjectOutputStream oos = new ObjectOutputStream(baos);
					oos.writeObject(cols);
					oos.close();
					ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
					ObjectInputStream ois = new ObjectInputStream(bais);
					clone = ois.readObject();
					ois.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}

			return (Map<Integer, Column>) clone;
		}

		@Override
		public String toString() {
			return "Tbname:" + this.tbname + ",Dbname:" + this.dbname + ",Cols:" + this.cols;
		}
	}

	// column class
	static class Column implements Serializable {

		/**
		 * 默认序列化标识,不实现无法序列化和反序列化
		 */
		private static final long serialVersionUID = 1L;

		private String colname;

		private int colindex;

		private int colType;

		private int metaData;

		private boolean empty;

		private boolean contain;

		private int scale;

		private int precision;

		private Object value;

		private int length;

		private boolean primaryKey;

		public String getColname() {
			return colname;
		}

		public void setColname(String colname) {
			this.colname = colname;
		}

		public int getColindex() {
			return colindex;
		}

		public void setColindex(int colindex) {
			this.colindex = colindex;
		}

		public int getColType() {
			return colType;
		}

		public void setColType(int colType) {
			this.colType = colType;
		}

		public int getLength() {
			return length;
		}

		public void setLength(int length) {
			this.length = length;
		}

		public boolean isPrimaryKey() {
			return primaryKey;
		}

		public void setPrimaryKey(boolean primaryKey) {
			this.primaryKey = primaryKey;
		}

		public void setMetaData(CodeStream cs) {
			switch (this.colType) {
			case MYSQL_TYPE_FLOAT:
			case MYSQL_TYPE_DOUBLE:
			case MYSQL_TYPE_TINY_BLOB:
			case MYSQL_TYPE_BLOB:
			case MYSQL_TYPE_MEDIUM_BLOB:
			case MYSQL_TYPE_LONG_BLOB:
				this.metaData = cs.nextInt(1);
				break;
			case MYSQL_TYPE_BIT:
			case MYSQL_TYPE_VARCHAR:
				// Little-endian
				this.metaData = cs.nextInt(2, true);
				break;
			case MYSQL_TYPE_NEWDECIMAL:
			case MYSQL_TYPE_SET:
			case MYSQL_TYPE_ENUM:
			case MYSQL_TYPE_STRING:
				// Big-endian
				this.metaData = cs.nextInt(2, false);
				break;
			case MYSQL_TYPE_TIME2:
			case MYSQL_TYPE_DATETIME2:
			case MYSQL_TYPE_TIMESTAMP2:
				this.metaData = cs.nextInt(1);
				break;
			default:
				this.metaData = 0;
			}
		}

		public int getMetaData() {
			return metaData;
		}

		public boolean isEmpty() {
			return empty;
		}

		public void setEmpty(boolean empty) {
			this.empty = empty;
		}

		public boolean isContain() {
			return contain;
		}

		public void setContain(boolean contain) {
			this.contain = contain;
		}

		public Object getValue() {
			return value;
		}

		public void setValue(Object value) {
			this.value = value;
		}

		public int getScale() {
			return scale;
		}

		public void setScale(int scale) {
			this.scale = scale;
		}

		public int getPrecision() {
			return precision;
		}

		public void setPrecision(int precision) {
			this.precision = precision;
		}

		private int getDecimalHexSize(int meta) {
			// table_map_event中的metadata为2个字节，它会记录该类型的精度和标度，其中第一个字节表示标度，第二个字节表示精度
			// 取第二个字节
			final int scale = meta & 0xFF;
			this.setScale(scale);
			// 右移8位，取第一个字节
			final int precision = meta >> 8;
			this.setPrecision(precision);
			final int x = precision - scale;
			final int ipDigits = x / DIGITS_PER_4BYTES;
			final int fpDigits = scale / DIGITS_PER_4BYTES;
			final int ipDigitsX = x - ipDigits * DIGITS_PER_4BYTES;
			final int fpDigitsX = scale - fpDigits * DIGITS_PER_4BYTES;
			return (ipDigits << 2) + DECIMAL_BINARY_SIZE[ipDigitsX] + (fpDigits << 2) + DECIMAL_BINARY_SIZE[fpDigitsX];
		}

		@Override
		public String toString() {
			return this.colname + ":" + this.value + ",ct:" + this.colType + ":" + this.metaData;
		}

		public boolean equals(Column other) {
			if (this == other) {
				return true;
			}
			if (other == null) {
				return false;
			}
			if (getClass() != other.getClass()) {
				return false;
			}
			if (this.value == null) {
				if (other.value != null) {
					return false;
				}
			} else if (!this.value.equals(other.value)) {
				return false;
			}
			return true;
		}
	}

	// codestream class
	static class CodeStream {

		private byte[] bt;

		private int pos;

		public CodeStream(byte[] bt) {
			this.bt = bt;
		}

		public byte[] getBt() {
			return bt;
		}

		public void setBt(byte[] bt) {
			this.bt = bt;
		}

		public int getPos() {
			return pos;
		}

		public void setPos(int pos) {
			this.pos = pos;
		}

		public int getAndIncrement(int len) {
			int cur = this.pos;
			this.pos += len;
			return cur;
		}

		private byte[] sub(int len) {
			return sub(len, false);
		}

		private byte[] sub(int len, boolean reverse) {
			byte[] temp = Arrays.copyOfRange(this.bt, this.getAndIncrement(len), this.pos);

			if (reverse) {
				byte[] res = new byte[temp.length];
				for (int i = 0; i < temp.length; i++) {
					res[i] = temp[temp.length - 1 - i];
				}

				return res;
			} else {
				return temp;
			}
		}

		public String nextCharsetString(int len) {
			return MysqlUtils.toCharsetString(sub(len));
		}

		public String nextHexString(int len) {
			return MysqlUtils.toHexString(sub(len));
		}

		public int nextInt(int len) {
			return nextInt(len, false);
		}

		public int nextInt() {
			return nextInt(1);
		}

		public int nextSignedInt(int length) {
			int r = 0;
			for (int i = 0; i < length; ++i) {
				final int v = nextInt();
				r |= (v << (i << 3));
				if ((i == length - 1) && ((v & 0x80) == 0x80)) {
					for (int j = length; j < 4; j++) {
						r |= (255 << (j << 3));
					}
				}
			}
			return r;
		}

		public long nextSignedLong(int length) {
			long r = 0;
			for (int i = 0; i < length; ++i) {
				final long v = nextLong();
				r |= (v << (i << 3));
				if ((i == length - 1) && ((v & 0x80) == 0x80)) {
					for (int j = length; j < 8; j++) {
						r |= (255 << (j << 3));
					}
				}
			}
			return r;
		}

		public int nextInt(int len, boolean reverse) {
			return MysqlUtils.toInt(sub(len, reverse));
		}

		public CodeStream subCS(int len) {
			byte[] sbt = sub(len);
			CodeStream sub = new CodeStream(sbt);
			return sub;
		}

		@Override
		public String toString() {
			int line = 0;
			StringBuilder buf = new StringBuilder();
			for (byte d : bt) {
				if (line % HEX == 0) {
					buf.append(String.format("%05x: ", line));
				}
				buf.append(String.format("%02x  ", d));
				line++;
				if (line % HEX == 0) {
					buf.append("\n");
				}
			}
			buf.append("\n");
			return buf.toString();
		}

		public long nextPackedLong() {
			final int v = this.nextInt(1);
			if (v < 251) {
				return Long.valueOf(v);
			} else if (v == 251) {
				return 0L;
			} else if (v == 252) {
				return Long.valueOf(this.nextInt(2));
			} else if (v == 253) {
				return Long.valueOf(this.nextInt(3));
			} else if (v == 254) {
				return Long.valueOf(this.nextInt(8));
			} else {
				throw new RuntimeException("assertion failed, should NOT reach here");
			}
		}

		public char[] nextCharArray(int len) {
			return MysqlUtils.toBinString(sub(len, true)).toCharArray();
		}

		public String nextDate(int len) {
			return MysqlUtils.toDate(sub(len, true));
		}

		public BigDecimal nextDecimal(int precision, int scale, int len) {
			return MysqlUtils.toDecimal(precision, scale, sub(len));
		}

		public long nextLong(int len, boolean reverse) {
			return MysqlUtils.toLong(sub(len, false));
		}

		public long nextLong(int len) {
			return MysqlUtils.toLong(sub(len, true));
		}

		public long nextLong() {
			return MysqlUtils.toLong(sub(1));
		}

		public Object nextVal(Column c) {
			switch (c.colType) {
			case MYSQL_TYPE_LONG:
				return nextSignedInt(4);
			case MYSQL_TYPE_DATE:
				return nextDate(3);
			case MYSQL_TYPE_NEWDECIMAL:
				return nextDecimal(c.getPrecision(), c.getScale(), c.getDecimalHexSize(c.getMetaData())).toString();
			case MYSQL_TYPE_LONGLONG:
				return nextSignedLong(8);
			case MYSQL_TYPE_ENUM:
				return nextInt(c.getLength());
			case MYSQL_TYPE_VARCHAR:
			case MYSQL_TYPE_VAR_STRING:
				return nextCharsetString(c.getMetaData() < 256 ? nextInt(1) : nextInt(2, true));
			case MYSQL_TYPE_STRING:
				return nextCharsetString(c.getLength() < 256 ? nextInt(1) : nextInt(2, true));
			case MYSQL_TYPE_SHORT:
				return nextSignedInt(2);
			case MYSQL_TYPE_TINY:
				return nextSignedInt(1);
			case MYSQL_TYPE_INT24:
				return nextSignedInt(3);
			case MYSQL_TYPE_FLOAT:
				return Float.intBitsToFloat(nextInt(4));
			case MYSQL_TYPE_DOUBLE:
				return Double.longBitsToDouble(nextLong(8));
			case MYSQL_TYPE_YEAR:
				return MysqlUtils.toYear(nextInt(1));
			case MYSQL_TYPE_TIME:
				return MysqlUtils.toTime(nextInt(3));
			case MYSQL_TYPE_DATETIME:
				return MysqlUtils.toDatetime(nextLong(8));
			case MYSQL_TYPE_TIMESTAMP:
				return MysqlUtils.toTimestamp(nextLong(4));
			// case MYSQL_TYPE_SET: columns.add(SetColumn.valueOf(is.readLong(length))); break;
			// case MYSQL_TYPE_BIT:
			// final int bitLength = (meta >> 8) * 8 + (meta & 0xFF);
			// columns.add(is.readBit(bitLength, false));
			// break;
			// case MYSQL_TYPE_BLOB:
			// final int blobLength = is.readInt(meta);
			// columns.add(BlobColumn.valueOf(is.readBytes(blobLength)));
			// break;
			// case MYSQL_TYPE_TIME2:
			// final int value1 = is.readInt(3, false);
			// final int nanos1 = is.readInt((meta + 1) / 2, false);
			// columns.add(Time2Column.valueOf(MySQLUtils.toTime2(value1, nanos1)));
			// break;
			case MYSQL_TYPE_DATETIME2:
				final long value2 = nextLong(5, false);
				final int nanos2 = nextInt((c.getMetaData() + 1) / 2, false);
				return new Timestamp(MysqlUtils.toDatetime2(value2, nanos2).getTime());
			case MYSQL_TYPE_TIMESTAMP2:
				final long value3 = nextLong(4, false);
				final int nanos3 = nextInt((c.getMetaData() + 1) / 2, false);
				return MysqlUtils.toTimestamp2(value3, nanos3);
			default:
				throw new RuntimeException("assertion failed, unknown column type: " + c.getColType());
			}
		}
	}

	// record class
	static class Record {

		private String tbname;

		private int eventType;

		private boolean generateReverseSql;

		private boolean primaryKey;

		private Map<Integer, Column> vmap = new HashMap<Integer, Column>();

		private Map<Integer, Column> vmapUpdate = new HashMap<Integer, Column>();

		public String getTbname() {
			return tbname;
		}

		public void setTbname(String tbname) {
			this.tbname = tbname;
		}

		public int getEventType() {
			return eventType;
		}

		public void setEventType(int eventType) {
			this.eventType = eventType;
		}

		public boolean isGenerateReverseSql() {
			return generateReverseSql;
		}

		public void setGenerateReverseSql(boolean generateReverseSql) {
			this.generateReverseSql = generateReverseSql;
		}

		public Map<Integer, Column> getVmap() {
			return vmap;
		}

		public void setVmap(Map<Integer, Column> vmap) {
			this.vmap = vmap;
		}

		public Map<Integer, Column> getVmapUpdate() {
			return vmapUpdate;
		}

		public void setVmapUpdate(Map<Integer, Column> vmapUpdate) {
			this.vmapUpdate = vmapUpdate;
		}

		public boolean isPrimaryKey() {
			return primaryKey;
		}

		public void setPrimaryKey(boolean primaryKey) {
			this.primaryKey = primaryKey;
		}

		private String parseInsertSQL() {
			String sql = "insert into " + this.tbname + " (#collist#) values (#vallist#);";
			String collist = "";
			String vallist = "";

			for (int i = 1; i <= vmap.size(); i++) {
				Column col = vmap.get(i);

				if (col.contain) {
					String tail;
					if (i == vmap.size()) {
						tail = "";
					} else {
						tail = ",";
					}

					collist += col.getColname() + tail;
					if (!col.empty) {
						vallist += formatVal(col.getValue()) + tail;
					} else {
						vallist += "null" + tail;
					}
				}
			}

			return sql.replaceFirst("#collist#", collist).replaceAll("#vallist#", vallist);
		}

		private String parseUpdateSQL() {
			String sql = "update " + this.tbname + " set #setlist# where #wherelist#;";
			String setlist = "";
			String wherelist = "";

			Map<Integer, Column> wheres = null;
			Map<Integer, Column> sets = null;

			if (generateReverseSql) {
				wheres = vmapUpdate;
				sets = vmap;
			} else {
				wheres = vmap;
				sets = vmapUpdate;
			}

			int index = 0;
			for (int i = 1; i <= wheres.size(); i++) {
				Column col = wheres.get(i);

				String pre = "";
				if (index > 0) {
					pre = " and ";
				}

				if (col.contain && !((simple && primaryKey) && !col.primaryKey)) {
					wherelist += pre + col.getColname() + " = " + ((!col.empty) ? formatVal(col.getValue()) : "null");
					index++;
				}
			}

			index = 0;
			for (int i = 1; i <= sets.size(); i++) {
				Column colb = sets.get(i);

				String pre = "";
				if (index > 0) {
					pre = ",";
				}
				if (colb.contain && !(onlychange && colb.equals(wheres.get(i)))) {
					setlist += pre + colb.getColname() + " = " + ((!colb.empty) ? formatVal(colb.getValue()) : "null");
					index++;
				}
			}

			return sql.replaceFirst("#setlist#", setlist).replaceAll("#wherelist#", wherelist);
		}

		private String parseDeleteSQL() {
			String sql = "delete from " + this.tbname + " where delete_condition;";
			String deleteCondition = "";

			int index = 0;
			for (int i = 1; i <= vmap.size(); i++) {
				Column col = vmap.get(i);

				String pre = "";
				if (index > 0) {
					pre = " and ";
				}

				if (col.contain && (!simple || col.primaryKey)) {
					deleteCondition += pre + col.getColname() + " = " + ((!col.empty) ? formatVal(col.getValue()) : "null");
					index++;
				}
			}

			return sql.replaceFirst("delete_condition", deleteCondition);
		}

		private String formatVal(Object val) {
			if (val instanceof Integer || val instanceof Long || val instanceof Double || val instanceof Float) {
				return String.valueOf(val);
			} else {
				return "'" + val + "'";
			}
		}

		@Override
		public String toString() {
			switch (this.eventType) {
			case UPDATE_ROWS_EVENT:
				return parseUpdateSQL();
			case DELETE_ROWS_EVENT:
				return generateReverseSql ? parseInsertSQL() : parseDeleteSQL();
			case WRITE_ROWS_EVENT:
				return generateReverseSql ? parseDeleteSQL() : parseInsertSQL();
			default:
			}
			return "";
		}
	}

	// codestream class
	static class MysqlUtils {

		public static String toCharsetString(byte[] bt) {
			return toCharsetString(bt, "utf-8");
		}

		public static String toCharsetString(byte[] bt, String charset) {
			try {
				return new String(bt, charset);
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
			return "";
		}

		public static String toHexString(byte[] bt) {
			StringBuilder buf = new StringBuilder();
			for (byte d : bt) {
				buf.append(String.format("%02x", d));
			}
			return buf.toString();
		}

		public static int toInt(byte[] data) {
			return toInt(data, 0, data.length);
		}

		public static int toInt(byte[] data, int offset, int length) {
			int r = 0;
			for (int i = offset; i < (offset + length); i++) {
				final byte b = data[i];
				r = (r << 8) | (b >= 0 ? (int) b : (b + 256));
			}
			return r;
		}

		public static long toLong(byte[] data, int offset, int length) {
			long r = 0;
			for (int i = offset; i < (offset + length); i++) {
				final byte b = data[i];
				r = (r << 8) | (b >= 0 ? (int) b : (b + 256));
			}
			return r;
		}

		public static long toLong(byte[] data) {
			return toLong(data, 0, data.length);
		}

		public static String toBinString(byte[] bt) {

			String[] binaryArray = { "0000", "0001", "0010", "0011", "0100", "0101", "0110", "0111", "1000", "1001", "1010", "1011", "1100",
					"1101", "1110", "1111" };

			String outStr = "";
			int pos = 0;
			for (byte b : bt) {
				// 高四位
				pos = (b & 0xF0) >> 4;
				outStr += binaryArray[pos];
				// 低四位
				pos = b & 0x0F;
				outStr += binaryArray[pos];
			}
			return outStr;
		}

		public static BigDecimal toDecimal(int precision, int scale, byte[] value) {
			//
			final boolean positive = (value[0] & 0x80) == 0x80;
			value[0] ^= 0x80;
			if (!positive) {
				for (int i = 0; i < value.length; i++) {
					value[i] ^= 0xFF;
				}
			}

			final int x = precision - scale;
			final int ipDigits = x / DIGITS_PER_4BYTES;
			final int ipDigitsX = x - ipDigits * DIGITS_PER_4BYTES;
			final int ipSize = (ipDigits << 2) + DECIMAL_BINARY_SIZE[ipDigitsX];
			int offset = DECIMAL_BINARY_SIZE[ipDigitsX];
			BigDecimal ip = offset > 0 ? BigDecimal.valueOf(toInt(value, 0, offset)) : BigDecimal.ZERO;
			for (; offset < ipSize; offset += 4) {
				final int i = toInt(value, offset, 4);
				ip = ip.movePointRight(DIGITS_PER_4BYTES).add(BigDecimal.valueOf(i));
			}

			int shift = 0;
			BigDecimal fp = BigDecimal.ZERO;
			for (; shift + DIGITS_PER_4BYTES <= scale; shift += DIGITS_PER_4BYTES, offset += 4) {
				final int i = toInt(value, offset, 4);
				fp = fp.add(BigDecimal.valueOf(i).movePointLeft(shift + DIGITS_PER_4BYTES));
			}
			if (shift < scale) {
				final int i = toInt(value, offset, DECIMAL_BINARY_SIZE[scale - shift]);
				fp = fp.add(BigDecimal.valueOf(i).movePointLeft(scale));
			}

			return positive ? POSITIVE_ONE.multiply(ip.add(fp)) : NEGATIVE_ONE.multiply(ip.add(fp));
		}

		public static Timestamp toTimestamp2(long seconds, int nanos) {
			final java.sql.Timestamp r = new java.sql.Timestamp(seconds * 1000L);
			r.setNanos(nanos * 1000);
			return r;
		}

		public static String toDate(byte[] bt) {
			// 1~15位: 存储年(共15位),16~19位: 存储月(共4位),20~24位: 存储日(共5位)
			long l = toLong(bt);
			int y = (int) (l >> 9);
			int m = (int) (l >> 5 & 0xF);
			int d = (int) (l & 0x1F);
			return y + ":" + m + ":" + d;
		}

		public static java.util.Date toDatetime2(long value, int nanos) {
			final long x = (value >> 22) & 0x1FFFFL;
			final int year = (int) (x / 13);
			final int month = (int) (x % 13);
			final int day = ((int) (value >> 17)) & 0x1F;
			final int hour = ((int) (value >> 12)) & 0x1F;
			final int minute = ((int) (value >> 6)) & 0x3F;
			final int second = ((int) (value >> 0)) & 0x3F;
			final Calendar c = Calendar.getInstance();
			c.set(year, month - 1, day, hour, minute, second);
			c.set(Calendar.MILLISECOND, 0);
			final long millis = c.getTimeInMillis();
			return new java.util.Date(millis + (nanos / 1000000));
		}

		public static java.sql.Timestamp toTimestamp(long seconds) {
			return new java.sql.Timestamp(seconds * 1000L);
		}

		public static int toYear(int value) {
			return 1900 + value;
		}

		public static java.sql.Date toDate(int value) {
			final int d = value % 32;
			value >>>= 5;
			final int m = value % 16;
			final int y = value >> 4;
			final Calendar cal = Calendar.getInstance();
			cal.clear();
			cal.set(y, m - 1, d);
			return new java.sql.Date(cal.getTimeInMillis());
		}

		public static java.sql.Time toTime(int value) {
			final int s = (int) (value % 100);
			value /= 100;
			final int m = (int) (value % 100);
			final int h = (int) (value / 100);
			final Calendar c = Calendar.getInstance();
			c.set(1970, 0, 1, h, m, s);
			c.set(Calendar.MILLISECOND, 0);
			return new java.sql.Time(c.getTimeInMillis());
		}

		public static java.sql.Time toTime2(int value, int nanos) {
			final int h = (value >> 12) & 0x3FF;
			final int m = (value >> 6) & 0x3F;
			final int s = (value >> 0) & 0x3F;
			final Calendar c = Calendar.getInstance();
			c.set(1970, 0, 1, h, m, s);
			c.set(Calendar.MILLISECOND, 0);
			final long millis = c.getTimeInMillis();
			return new java.sql.Time(millis + (nanos / 1000000));
		}

		public static java.util.Date toDatetime(long value) {
			final int second = (int) (value % 100);
			value /= 100;
			final int minute = (int) (value % 100);
			value /= 100;
			final int hour = (int) (value % 100);
			value /= 100;
			final int day = (int) (value % 100);
			value /= 100;
			final int month = (int) (value % 100);
			final int year = (int) (value / 100);
			final Calendar c = Calendar.getInstance();
			c.set(year, month - 1, day, hour, minute, second);
			c.set(Calendar.MILLISECOND, 0);
			return c.getTime();
		}
	}

	public static void main(String[] args) {
		user = "root";
		password = "zte";
		url = "jdbc:mysql://localhost:3306/lt?useCursorFetch=true&useSSL=false";
		generateReverseSql = false;
		simple = true;
		onlychange = true;
		byte[] bt = readFile("D:\\mysql-5.7.18-winx64\\data\\mysqlbin-log.000001");
		parse(bt);
	}
}
