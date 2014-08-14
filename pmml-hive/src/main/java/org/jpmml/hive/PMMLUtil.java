/*
 * Copyright (c) 2014 Villu Ruusmann
 *
 * This file is part of JPMML-Hive
 *
 * JPMML-Hive is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * JPMML-Hive is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with JPMML-Hive.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.jpmml.hive;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.dmg.pmml.DataField;
import org.dmg.pmml.DataType;
import org.dmg.pmml.FieldName;
import org.dmg.pmml.OutputField;
import org.jpmml.evaluator.Evaluator;
import org.jpmml.evaluator.EvaluatorUtil;
import org.jpmml.evaluator.FieldValue;
import org.jpmml.runtime.ModelEvaluatorCache;

public class PMMLUtil {

	private PMMLUtil(){
	}

	/**
	 * @see #initializeSimpleResult(Class)
	 * @see #initializeComplexResult(Class)
	 */
	static
	public ObjectInspector[] initializeArguments(Class<? extends GenericUDF> clazz, ObjectInspector[] inspectors) throws UDFArgumentException {
		Evaluator evaluator = getEvaluator(clazz);

		if(inspectors.length == 1){
			ObjectInspector inspector = inspectors[0];

			ObjectInspector.Category category = inspector.getCategory();
			switch(category){
				case STRUCT:
					return initializeStruct(evaluator, inspector);
				default:
					return initializePrimitiveList(evaluator, inspectors);
			}
		}

		return initializePrimitiveList(evaluator, inspectors);
	}

	static
	private ObjectInspector[] initializeStruct(Evaluator evaluator, ObjectInspector inspector) throws UDFArgumentException {
		StructObjectInspector structInspector = (StructObjectInspector)inspector;

		List<FieldName> activeFields = evaluator.getActiveFields();
		for(FieldName activeField : activeFields){
			DataField field = evaluator.getDataField(activeField);

			StructField structField;

			try {
				structField = structInspector.getStructFieldRef(activeField.getValue());
			} catch(Exception e){
				throw new UDFArgumentTypeException(0, "Missing struct field \"" + activeField.getValue() + "\"");
			}

			ObjectInspector structFieldInspector = structField.getFieldObjectInspector();

			ObjectInspector.Category structFieldCategory = structFieldInspector.getCategory();
			switch(structFieldCategory){
				case PRIMITIVE:
					break;
				default:
					throw new UDFArgumentTypeException(0, "Struct field \"" + activeField.getValue() + "\": Expected " + ObjectInspector.Category.PRIMITIVE + " type, got " + structFieldCategory + " type");
			}

			DataType dataType = getDataType(structFieldInspector);
			if(!isConvertible(dataType, field.getDataType())){
				throw new UDFArgumentTypeException(0, "Struct field \"" + activeField.getValue() + "\": Cannot convert " + dataType + " to " + field.getDataType());
			}
		}

		return new ObjectInspector[]{structInspector};
	}

	static
	private ObjectInspector[] initializePrimitiveList(Evaluator evaluator, ObjectInspector[] inspectors) throws UDFArgumentException {

		List<FieldName> activeFields = evaluator.getActiveFields();
		if(inspectors.length != activeFields.size()){
			throw new UDFArgumentLengthException("Expected " + activeFields.size() + " arguments, got " + inspectors.length + " arguments");
		}

		int i = 0;

		for(FieldName activeField : activeFields){
			DataField field = evaluator.getDataField(activeField);

			ObjectInspector inspector = inspectors[i];

			ObjectInspector.Category category = inspector.getCategory();
			switch(category){
				case PRIMITIVE:
					break;
				default:
					throw new UDFArgumentTypeException(i, "Expected " + ObjectInspector.Category.PRIMITIVE + " type, got " + category + " type");
			}

			DataType dataType = getDataType(inspector);
			if(!isConvertible(dataType, field.getDataType())){
				throw new UDFArgumentTypeException(i, "Cannot convert " + dataType + " to " + field.getDataType());
			}

			i++;
		}

		return inspectors;
	}

	/**
	 * @see #initializeArguments(Class, ObjectInspector[])
	 */
	static
	public PrimitiveObjectInspector initializeSimpleResult(Class<? extends GenericUDF> clazz) throws UDFArgumentException {
		Evaluator evaluator = getEvaluator(clazz);

		FieldName targetField = evaluator.getTargetField();

		DataField field = evaluator.getDataField(targetField);

		return toObjectInspector(field.getDataType());
	}

	/**
	 * @see #initializeArguments(Class, ObjectInspector[])
	 */
	static
	public StructObjectInspector initializeComplexResult(Class<? extends GenericUDF> clazz) throws UDFArgumentException {
		Evaluator evaluator = getEvaluator(clazz);

		List<String> fieldNames = Lists.newArrayList();
		List<ObjectInspector> fieldInspectors = Lists.newArrayList();

		List<FieldName> targetFields = evaluator.getTargetFields();
		for(FieldName targetField : targetFields){
			DataField field = evaluator.getDataField(targetField);

			fieldNames.add(targetField.getValue());
			fieldInspectors.add(toObjectInspector(field.getDataType()));
		}

		List<FieldName> outputFields = evaluator.getOutputFields();
		for(FieldName outputField : outputFields){
			OutputField field = evaluator.getOutputField(outputField);

			fieldNames.add(outputField.getValue());
			fieldInspectors.add(toObjectInspector(field.getDataType()));
		}

		return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldInspectors);
	}

	static
	public String getDisplayString(String name, String[] strings){
		return name + "(" + Arrays.toString(strings) + ")";
	}

	/**
	 * @see #initializeArguments(Class, ObjectInspector[])
	 * @see #initializeSimpleResult(Class)
	 */
	static
	public Object evaluateSimple(Class<? extends GenericUDF> clazz, ObjectInspector[] inspectors, GenericUDF.DeferredObject[] objects) throws HiveException {
		Evaluator evaluator = getEvaluator(clazz);

		Map<FieldName, FieldValue> arguments = loadArguments(evaluator, inspectors, objects);

		Map<FieldName, ?> result = evaluator.evaluate(arguments);

		Object targetValue = result.get(evaluator.getTargetField());

		return EvaluatorUtil.decode(targetValue);
	}

	/**
	 * @see #initializeArguments(Class, ObjectInspector[])
	 * @see #initializeComplexResult(Class)
	 */
	static
	public Object evaluateComplex(Class<? extends GenericUDF> clazz, ObjectInspector[] inspectors, GenericUDF.DeferredObject[] objects) throws HiveException {
		Evaluator evaluator = getEvaluator(clazz);

		Map<FieldName, FieldValue> arguments = loadArguments(evaluator, inspectors, objects);

		Map<FieldName, ?> result = evaluator.evaluate(arguments);

		return storeResult(evaluator, result);
	}

	static
	private Map<FieldName, FieldValue> loadArguments(Evaluator evaluator, ObjectInspector[] inspectors, GenericUDF.DeferredObject[] objects) throws HiveException {

		if(inspectors.length == 1){
			ObjectInspector inspector = inspectors[0];

			ObjectInspector.Category category = inspector.getCategory();
			switch(category){
				case STRUCT:
					return loadStruct(evaluator, inspectors[0], objects[0]);
				default:
					return loadPrimitiveList(evaluator, inspectors, objects);
			}
		}

		return loadPrimitiveList(evaluator, inspectors, objects);
	}

	static
	private Map<FieldName, FieldValue> loadStruct(Evaluator evaluator, ObjectInspector inspector, GenericUDF.DeferredObject object) throws HiveException {
		Map<FieldName, FieldValue> result = Maps.newLinkedHashMap();

		StructObjectInspector structInspector = (StructObjectInspector)inspector;

		Object structObject = object.get();

		List<FieldName> activeFields = evaluator.getActiveFields();
		for(FieldName activeField : activeFields){
			StructField structField = structInspector.getStructFieldRef(activeField.getValue());

			PrimitiveObjectInspector primitiveObjectInspector = (PrimitiveObjectInspector)structField.getFieldObjectInspector();

			Object primitiveObject = structInspector.getStructFieldData(structObject, structField);

			FieldValue value = EvaluatorUtil.prepare(evaluator, activeField, primitiveObjectInspector.getPrimitiveJavaObject(primitiveObject));

			result.put(activeField, value);
		}

		return result;
	}

	static
	private Map<FieldName, FieldValue> loadPrimitiveList(Evaluator evaluator, ObjectInspector[] inspectors, GenericUDF.DeferredObject[] objects) throws HiveException {
		Map<FieldName, FieldValue> result = Maps.newLinkedHashMap();

		int i = 0;

		List<FieldName> activeFields = evaluator.getActiveFields();
		for(FieldName activeField : activeFields){
			PrimitiveObjectInspector primitiveInspector = (PrimitiveObjectInspector)inspectors[i];

			Object primitiveObject = objects[i].get();

			FieldValue value = EvaluatorUtil.prepare(evaluator, activeField, primitiveInspector.getPrimitiveJavaObject(primitiveObject));

			result.put(activeField, value);

			i++;
		}

		return result;
	}

	static
	private Object storeResult(Evaluator evaluator, Map<FieldName, ?> result){
		return storeStruct(evaluator, result);
	}

	static
	private Object[] storeStruct(Evaluator evaluator, Map<FieldName, ?> result){
		List<Object> resultStruct = Lists.newArrayList();

		List<FieldName> targetFields = evaluator.getTargetFields();
		for(FieldName targetField : targetFields){
			resultStruct.add(EvaluatorUtil.decode(result.get(targetField)));
		}

		List<FieldName> outputFields = evaluator.getOutputFields();
		for(FieldName outputField : outputFields){
			resultStruct.add(result.get(outputField));
		}

		return resultStruct.toArray(new Object[resultStruct.size()]);
	}

	static
	private DataType getDataType(ObjectInspector inspector) throws UDFArgumentException {
		PrimitiveObjectInspector primitiveInspector = (PrimitiveObjectInspector)inspector;

		PrimitiveObjectInspector.PrimitiveCategory primitiveCategory = primitiveInspector.getPrimitiveCategory();
		switch(primitiveCategory){
			case STRING:
			case VARCHAR:
				return DataType.STRING;
			case BYTE:
			case SHORT:
			case INT:
				return DataType.INTEGER;
			case FLOAT:
				return DataType.FLOAT;
			case DOUBLE:
				return DataType.DOUBLE;
			case BOOLEAN:
				return DataType.BOOLEAN;
			default:
				throw new UDFArgumentException();
		}
	}

	static
	private PrimitiveObjectInspector toObjectInspector(DataType dataType) throws UDFArgumentException {

		switch(dataType){
			case STRING:
				return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
			case INTEGER:
				return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
			case FLOAT:
				return PrimitiveObjectInspectorFactory.javaFloatObjectInspector;
			case DOUBLE:
				return PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
			case BOOLEAN:
				return PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
			default:
				throw new UDFArgumentException();
		}
	}

	static
	private boolean isConvertible(DataType left, DataType right){

		// A string can be parsed to any PMML data type
		if((left).equals(DataType.STRING)){
			return true;
		} // End if

		if((left).equals(right)){
			return true;
		}

		switch(left){
			case INTEGER:
				switch(right){
					case FLOAT:
					case DOUBLE:
						return true;
					default:
						return false;
				}
			case FLOAT:
				switch(right){
					case DOUBLE:
						return true;
					default:
						return false;
				}
			default:
				return false;
		}
	}

	static
	private Evaluator getEvaluator(Class<? extends GenericUDF> clazz) throws UDFArgumentException {

		try {
			return PMMLUtil.evaluatorCache.get(clazz);
		} catch(Exception e){
			throw new UDFArgumentException(e);
		}
	}

	private static final ModelEvaluatorCache evaluatorCache = new ModelEvaluatorCache(CacheBuilder.newBuilder());
}
