JPMML-Hive
==========

PMML evaluator library for the Apache Hive data warehouse software (http://hive.apache.org).

# Features #

* Full support for PMML specification versions 3.0 through 4.2. The evaluation is handled by the [JPMML-Evaluator] (https://github.com/jpmml/jpmml-evaluator) library.

# Prerequisites #

* Apache Hive version 0.12.0 or newer.

# Overview #

A working JPMML-Hive setup consists of a library JAR file and a number of model JAR files. The library JAR is centered around the utility class `org.jpmml.hive.PMMLUtil`, which provides Hive compliant utility methods for handling most common PMML evaluation scenarios. A model JAR file contains one or more model launcher classes and a PMML resource.

The main responsibility of a model launcher class is to formalize the "public interface" of a PMML resource. A model launcher class must extend abstract Hive user-defined function (UDF) class `org.apache.hadoop.hive.ql.udf.generic.GenericUDF` and provide concrete implementations for the following methods:

* `#initialize(ObjectInspector[])`. The initialization of argument types is handled by the method `PMMLUtil#initializeArguments(Class, ObjectInspector[])`. The initialization of the result type is handled either by the method `PMMLUtil#initializeSimpleResult(Class)` or `PMMLUtil#handleComplexResult(Class)`.
* `#evaluate(GenericUDF.DeferredObject[])`. Handled either by the method `PMMLUtil#evaluateSimple(Class, ObjectInspector[], GenericUDF.DeferredObject[])` or `PMMLUtil#evaluateComplex(Class, ObjectInspector[], GenericUDF.DeferredObject[])`.
* `#getDisplayString(String[])`. Handled by the method `PMMLUtil#getDisplayString(Class, String[])`.

All in all, a typical model launcher class can be implemented in 15 to 20 lines of boilerplate-esque Java source code.

The example model JAR file contains a DecisionTree model for the "iris" dataset. This model is exposed in two ways. First, the model launcher class `org.jpmml.hive.DecisionTreeIrisSimple` defines a custom function that returns the PMML target field ("Species") as a string. Second, the model launcher class `org.jpmml.hive.DecisionTreeIrisComplex` defines a custom function that returns the PMML target field ("Species") together with four output fields ("Predicted_Species", "Probability_setosa", "Probability_versicolor", "Probability_virginica") as a `struct`.

# Installation #

Enter the project root directory and build using [Apache Maven] (http://maven.apache.org/):
```
mvn clean install
```

The build produces two JAR files:
* `pmml-hive/target/pmml-hive-runtime-1.0-SNAPSHOT.jar` - Library uber-JAR file. It contains the classes of the library JAR file `pmml-hive/target/pmml-hive-1.0-SNAPSHOT.jar`, plus all the classes of its transitive dependencies.
* `pmml-hive-example/target/pmml-hive-example-1.0-SNAPSHOT.jar` - Example model JAR file.

# Usage #

### Library

##### Installation

Add the library uber-JAR file to Hive classpath:
```
ADD JAR /tmp/pmml-hive-runtime-1.0-SNAPSHOT.jar;
```

### Example model

##### Installation

Add the example model JAR file to Hive classpath:
```
ADD JAR /tmp/pmml-hive-example-1.0-SNAPSHOT.jar;
```

Declare custom functions based on UDF implementation classes:
```
CREATE TEMPORARY FUNCTION iris_simple AS 'org.jpmml.hive.DecisionTreeIrisSimple';
CREATE TEMPORARY FUNCTION iris_complex AS 'org.jpmml.hive.DecisionTreeIrisComplex';
```

##### Usage

Execute a custom function using a list of scalar arguments:
```
SELECT iris_simple(5.1, 3.5, 1.4, 0.2);
```

Execute a custom function using a `struct` argument:
```
SELECT iris_simple(named_struct('Sepal_Length', 5.1, 'Sepal_Width', 3.5, 'Petal_Length', 1.4, 'Petal_Width', 0.2));
```

# License #

JPMML-Hive is dual-licensed under the [GNU Affero General Public License (AGPL) version 3.0] (http://www.gnu.org/licenses/agpl-3.0.html) and a commercial license.

# Additional information #

Please contact [info@openscoring.io] (mailto:info@openscoring.io)
