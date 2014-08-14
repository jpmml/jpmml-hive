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

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

@UDFType (
	deterministic = true
)
@Description (
	name = "iris_simple",
	value = "_FUNC_(Sepal_Length, Sepal_Width, Petal_Length, Petal_Width): Species"
)
public class DecisionTreeIrisSimple extends DecisionTreeIris {

	private ObjectInspector[] inspectors = null;


	@Override
	public ObjectInspector initialize(ObjectInspector[] parameterObjectInspectors) throws UDFArgumentException {
		this.inspectors = PMMLUtil.initializeArguments(DecisionTreeIris.class, parameterObjectInspectors);

		return PMMLUtil.initializeSimpleResult(DecisionTreeIris.class);
	}

	@Override
	public Object evaluate(DeferredObject[] parameterObjects) throws HiveException {
		return PMMLUtil.evaluateSimple(DecisionTreeIris.class, this.inspectors, parameterObjects);
	}
}