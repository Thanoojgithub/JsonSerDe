package com.hive.serde.json;

import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hive.hcatalog.data.JsonSerDe;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class MyJsonSerDe extends JsonSerDe {

	/**
	 * Apache commons logger
	 */
	private static final Log LOG = LogFactory.getLog(JsonSerDe.class.getName());

	/**
	 * The number of columns in the table this SerDe is being used with
	 */
	private int numColumns;

	/**
	 * List of column names in the table
	 */
	private List<String> columnNames;

	/**
	 * An ObjectInspector to be used as meta-data about a deSerialized row
	 */
	private StructObjectInspector rowOI;

	/**
	 * List of row objects
	 */
	private ArrayList<Object> row;

	/**
	 * List of column type information
	 */
	private List<TypeInfo> columnTypes;

	/**
	 * Initialize this SerDe with the system properties and table properties
	 */
	@Override
	public void initialize(Configuration sysProps, Properties tblProps) throws SerDeException {
		LOG.debug("Initializing JsonSerde");

		// Get the names of the columns for the table this SerDe is being used
		String columnNameProperty = tblProps.getProperty(serdeConstants.LIST_COLUMNS);
		columnNames = Arrays.asList(columnNameProperty.split(","));

		// Convert column types from text to TypeInfo objects
		String columnTypeProperty = tblProps.getProperty(serdeConstants.LIST_COLUMN_TYPES);
		columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
		assert columnNames.size() == columnTypes.size();
		numColumns = columnNames.size();

		// Create ObjectInspectors from the type information for each column
		List<ObjectInspector> columnOIs = new ArrayList<ObjectInspector>(columnNames.size());
		ObjectInspector oi;
		for (int c = 0; c < numColumns; c++) {
			oi = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(columnTypes.get(c));
			columnOIs.add(oi);
		}
		rowOI = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, columnOIs);

		// Create an empty row object to be reused during deSerialization
		row = new ArrayList<Object>(numColumns);
		for (int c = 0; c < numColumns; c++) {
			row.add(null);
		}

		LOG.debug("JsonSerde initialization complete");
	}

	/**
	 * Gets the ObjectInspector for a row deSerialized by this SerDe
	 */
	@Override
	public ObjectInspector getObjectInspector() throws SerDeException {
		return rowOI;
	}

	/**
	 * DeSerialize a JSON Object into a row for the table
	 */
	@Override
	public Object deserialize(Writable blob) throws SerDeException {
		Text rowText = (Text) blob;
		LOG.debug("DeSerialize row: " + rowText.toString());

		// Try parsing row into JSON object
		JSONObject jObj;
		try {
			jObj = new JSONObject(rowText.toString()) {
				/**
				 * In Hive column names are case insensitive, so lower-case all
				 * field names
				 */
				@Override
				public JSONObject put(String key, Object value) throws JSONException {
					return super.put(key.toLowerCase(), value);
				}
			};
		} catch (JSONException e) {
			// If row is not a JSON object, make the whole row NULL
			LOG.error("Row is not a valid JSON Object - JSONException: " + e.getMessage());
			return null;
		}

		// Loop over columns in table and set values
		String colName;
		Object value;
		for (int c = 0; c < numColumns; c++) {
			colName = columnNames.get(c);
			TypeInfo ti = columnTypes.get(c);

			try {
				// Get type-safe JSON values
				if (jObj.isNull(colName)) {
					value = null;
				} else if (ti.getTypeName().equalsIgnoreCase(serdeConstants.DOUBLE_TYPE_NAME)) {
					value = jObj.getDouble(colName);
				} else if (ti.getTypeName().equalsIgnoreCase(serdeConstants.BIGINT_TYPE_NAME)) {
					value = jObj.getLong(colName);
				} else if (ti.getTypeName().equalsIgnoreCase(serdeConstants.INT_TYPE_NAME)) {
					value = jObj.getInt(colName);
				} else if (ti.getTypeName().equalsIgnoreCase(serdeConstants.TINYINT_TYPE_NAME)) {
					value = Byte.valueOf(jObj.getString(colName));
				} else if (ti.getTypeName().equalsIgnoreCase(serdeConstants.FLOAT_TYPE_NAME)) {
					value = Float.valueOf(jObj.getString(colName));
				} else if (ti.getTypeName().equalsIgnoreCase(serdeConstants.BOOLEAN_TYPE_NAME)) {
					value = jObj.getBoolean(colName);
				} else if (ti.getTypeName().equalsIgnoreCase(serdeConstants.STRING_TYPE_NAME)) {
					value = jObj.getString(colName);
				} else if (ti.getTypeName().startsWith(serdeConstants.LIST_TYPE_NAME)) {
					// Copy to an Object array
					JSONArray jArray = jObj.getJSONArray(colName);
					Object[] newarr = new Object[jArray.length()];
					for (int i = 0; i < newarr.length; i++) {
						newarr[i] = jArray.get(i);
					}
					value = newarr;

				} else {
					// Fall back, just get an object
					value = jObj.get(colName);
				}
			} catch (JSONException e) {
				// If the column cannot be found, just make it a NULL value and
				// skip over it
				if (LOG.isDebugEnabled()) {
					LOG.debug("Column '" + colName + "' not found in row: " + rowText.toString() + " - JSONException: "
							+ e.getMessage());
				}
				value = null;
			}
			row.set(c, value);
		}

		return row;
	}

	@Override
	public Class<? extends Writable> getSerializedClass() {
		return Text.class;
	}

	/**
	 * Serializes a row of data into a JSON object
	 */
	@Override
	public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {
		StringBuilder sb = new StringBuilder();
		try {

			StructObjectInspector soi = (StructObjectInspector) objInspector;
			List<? extends StructField> structFields = soi.getAllStructFieldRefs();
			assert(columnNames.size() == structFields.size());
			if (obj == null) {
				sb.append("null");
			} else {
				sb.append(SerDeUtils.LBRACE);
				for (int i = 0; i < structFields.size(); i++) {
					if (i > 0) {
						sb.append(SerDeUtils.COMMA);
					}
					appendWithQuotes(sb, columnNames.get(i));
					System.out.println("MyJsonSerDe.serialize() - columnNames.get(i) - " + columnNames.get(i));
					sb.append(SerDeUtils.COLON);
					buildJSONString(sb, soi.getStructFieldData(obj, structFields.get(i)),
							structFields.get(i).getFieldObjectInspector());
				}
				sb.append(SerDeUtils.RBRACE);
			}

		} catch (IOException e) {
			LOG.warn("Error generating json text from object.", e);
			throw new SerDeException(e);
		}
		return new Text(sb.toString());
	}

	private static StringBuilder appendWithQuotes(StringBuilder sb, String value) {
		return sb == null ? null : sb.append(SerDeUtils.QUOTE).append(value).append(SerDeUtils.QUOTE);
	}

	private static void buildJSONString(StringBuilder sb, Object o, ObjectInspector oi) throws IOException {

		switch (oi.getCategory()) {
		case PRIMITIVE: {
			PrimitiveObjectInspector poi = (PrimitiveObjectInspector) oi;
			if (o == null) {
				sb.append("null");
			} else {
				switch (poi.getPrimitiveCategory()) {
				case BOOLEAN: {
					boolean b = ((BooleanObjectInspector) poi).get(o);
					sb.append(b ? "true" : "false");
					break;
				}
				case BYTE: {
					sb.append(((ByteObjectInspector) poi).get(o));
					break;
				}
				case SHORT: {
					sb.append(((ShortObjectInspector) poi).get(o));
					break;
				}
				case INT: {
					sb.append(((IntObjectInspector) poi).get(o));
					break;
				}
				case LONG: {
					sb.append(((LongObjectInspector) poi).get(o));
					break;
				}
				case FLOAT: {
					sb.append(((FloatObjectInspector) poi).get(o));
					break;
				}
				case DOUBLE: {
					sb.append(((DoubleObjectInspector) poi).get(o));
					break;
				}
				case STRING: {
					String s = SerDeUtils.escapeString(((StringObjectInspector) poi).getPrimitiveJavaObject(o));
					appendWithQuotes(sb, s);
					break;
				}
				case BINARY: {
					throw new IOException("JsonSerDe does not support BINARY type");
				}
				case DATE:
					Date d = ((DateObjectInspector) poi).getPrimitiveJavaObject(o);
					appendWithQuotes(sb, d.toString());
					break;
				case TIMESTAMP: {
					Timestamp t = ((TimestampObjectInspector) poi).getPrimitiveJavaObject(o);
					appendWithQuotes(sb, t.toString());
					break;
				}
				case DECIMAL:
					sb.append(((HiveDecimalObjectInspector) poi).getPrimitiveJavaObject(o));
					break;
				case VARCHAR:
					appendWithQuotes(sb, ((HiveVarcharObjectInspector) poi).getPrimitiveJavaObject(o).toString());
					break;
				case CHAR:
					// this should use HiveChar.getPaddedValue() but it's
					// protected; currently (v0.13)
					// HiveChar.toString() returns getPaddedValue()
					appendWithQuotes(sb, ((HiveCharObjectInspector) poi).getPrimitiveJavaObject(o).toString());
					break;
				default:
					throw new RuntimeException("Unknown primitive type: " + poi.getPrimitiveCategory());
				}
			}
			break;
		}
		case LIST: {
			ListObjectInspector loi = (ListObjectInspector) oi;
			ObjectInspector listElementObjectInspector = loi.getListElementObjectInspector();
			List<?> olist = loi.getList(o);
			if (olist == null) {
				sb.append("null");
			} else {
				sb.append(SerDeUtils.LBRACKET);
				for (int i = 0; i < olist.size(); i++) {
					if (i > 0) {
						sb.append(SerDeUtils.COMMA);
					}
					buildJSONString(sb, olist.get(i), listElementObjectInspector);
				}
				sb.append(SerDeUtils.RBRACKET);
			}
			break;
		}
		case MAP: {
			MapObjectInspector moi = (MapObjectInspector) oi;
			ObjectInspector mapKeyObjectInspector = moi.getMapKeyObjectInspector();
			ObjectInspector mapValueObjectInspector = moi.getMapValueObjectInspector();
			Map<?, ?> omap = moi.getMap(o);
			if (omap == null) {
				sb.append("null");
			} else {
				sb.append(SerDeUtils.LBRACE);
				boolean first = true;
				for (Object entry : omap.entrySet()) {
					if (first) {
						first = false;
					} else {
						sb.append(SerDeUtils.COMMA);
					}
					Map.Entry<?, ?> e = (Map.Entry<?, ?>) entry;
					StringBuilder keyBuilder = new StringBuilder();
					buildJSONString(keyBuilder, e.getKey(), mapKeyObjectInspector);
					String keyString = keyBuilder.toString().trim();
					if ((!keyString.isEmpty()) && (keyString.charAt(0) != SerDeUtils.QUOTE)) {
						appendWithQuotes(sb, keyString);
					} else {
						sb.append(keyString);
					}
					sb.append(SerDeUtils.COLON);
					buildJSONString(sb, e.getValue(), mapValueObjectInspector);
				}
				sb.append(SerDeUtils.RBRACE);
			}
			break;
		}
		case STRUCT: {
			StructObjectInspector soi = (StructObjectInspector) oi;
			List<? extends StructField> structFields = soi.getAllStructFieldRefs();
			if (o == null) {
				sb.append("null");
			} else {
				sb.append(SerDeUtils.LBRACE);
				for (int i = 0; i < structFields.size(); i++) {
					if (i > 0) {
						sb.append(SerDeUtils.COMMA);
					}
					appendWithQuotes(sb, structFields.get(i).getFieldName());
					sb.append(SerDeUtils.COLON);
					buildJSONString(sb, soi.getStructFieldData(o, structFields.get(i)),
							structFields.get(i).getFieldObjectInspector());
				}
				sb.append(SerDeUtils.RBRACE);
			}
			break;
		}
		case UNION: {
			UnionObjectInspector uoi = (UnionObjectInspector) oi;
			if (o == null) {
				sb.append("null");
			} else {
				sb.append(SerDeUtils.LBRACE);
				sb.append(uoi.getTag(o));
				sb.append(SerDeUtils.COLON);
				buildJSONString(sb, uoi.getField(o), uoi.getObjectInspectors().get(uoi.getTag(o)));
				sb.append(SerDeUtils.RBRACE);
			}
			break;
		}
		default:
			throw new RuntimeException("Unknown type in ObjectInspector!");
		}
	}
}
