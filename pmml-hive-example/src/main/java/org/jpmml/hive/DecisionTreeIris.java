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

import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;

abstract
public class DecisionTreeIris extends GenericUDF {

	@Override
	public String getDisplayString(String[] parameterStrings){
		return PMMLUtil.getDisplayString(getUdfName(), parameterStrings);
	}
}