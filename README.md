# my-spark-utilities

This repo contains a few useful Spark Scala utilities.

### Flatten Data Frame
This utility flattens out Spark dataframes that have a highly nested structure such as what might be expected from JSON source files

#### Usage: 
```
    import SparkUtilities.FlattenDataFrame.flattenDataFrame
    ...
    val df: DataFrame = flattenDataFrame(myDf: DataFrame, [true])
    # true indicates you want to expand out arrays as well
    # default is false 
```

There are a few smaller examples of nested JSONs in the `/src/main/resources` directory.

#### Example:

The file `sample.json` contains varying levels of nesting and arrays.
The schema has a mix of nested structures (`struct`s) and arrays.

```
root
 |-- boolean_key: string (nullable = true)
 |-- empty_string_translation: string (nullable = true)
 |-- key_with_description: string (nullable = true)
 |-- key_with_line-break: string (nullable = true)
 |-- nested: struct (nullable = true)
 |    |-- deeply: struct (nullable = true)
 |    |    |-- key: string (nullable = true)
 |    |-- key: string (nullable = true)
 |-- null_translation: string (nullable = true)
 |-- pluralized_key: struct (nullable = true)
 |    |-- one: string (nullable = true)
 |    |-- other: string (nullable = true)
 |    |-- zero: string (nullable = true)
 |-- sample_collection: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- simple_key: string (nullable = true)
 |-- unverified_key: string (nullable = true)

```

With `arrayFlatten` set to `false` (default)
```
|boolean_key|empty_string_translation|key_with_description|key_with_lineminus__break|null_translation|   sample_collection|          simple_key|      unverified_key|         nested__key| pluralized_key__one|pluralized_key__other|pluralized_key__zero| nested__deeply__key|
|-----------|------------------------|--------------------|-------------------------|----------------|--------------------|--------------------|--------------------|--------------------|--------------------|---------------------|--------------------|--------------------|
| --- true\n|                        |Check it out! Thi...|     This translations...|            null|[first item, seco...|Just a simple key...|This translation ...|This key is neste...|Only one pluraliz...| Wow, you have %s ...|You have no plura...|Wow, this key is ...|
```
All the structs are flattened, but the arrays are still intact.
```
root
 |-- boolean_key: string (nullable = true)
 |-- empty_string_translation: string (nullable = true)
 |-- key_with_description: string (nullable = true)
 |-- key_with_lineminus__break: string (nullable = true)
 |-- null_translation: string (nullable = true)
 |-- sample_collection: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- simple_key: string (nullable = true)
 |-- unverified_key: string (nullable = true)
 |-- nested__key: string (nullable = true)
 |-- pluralized_key__one: string (nullable = true)
 |-- pluralized_key__other: string (nullable = true)
 |-- pluralized_key__zero: string (nullable = true)
 |-- nested__deeply__key: string (nullable = true)
```

With `arrayFlatten` set to `true`
```
|-----------|------------------------|--------------------|-------------------------|----------------|--------------------|--------------------|--------------------|--------------------|---------------------|--------------------|-----------------|--------------------|
|boolean_key|empty_string_translation|key_with_description|key_with_lineminus__break|null_translation|          simple_key|      unverified_key|         nested__key| pluralized_key__one|pluralized_key__other|pluralized_key__zero|sample_collection| nested__deeply__key|
|-----------|------------------------|--------------------|-------------------------|----------------|--------------------|--------------------|--------------------|--------------------|---------------------|--------------------|-----------------|--------------------|
| --- true\n|                        |Check it out! Thi...|     This translations...|            null|Just a simple key...|This translation ...|This key is neste...|Only one pluraliz...| Wow, you have %s ...|You have no plura...|       first item|Wow, this key is ...|
| --- true\n|                        |Check it out! Thi...|     This translations...|            null|Just a simple key...|This translation ...|This key is neste...|Only one pluraliz...| Wow, you have %s ...|You have no plura...|      second item|Wow, this key is ...|
| --- true\n|                        |Check it out! Thi...|     This translations...|            null|Just a simple key...|This translation ...|This key is neste...|Only one pluraliz...| Wow, you have %s ...|You have no plura...|       third item|Wow, this key is ...|
|-----------|------------------------|--------------------|-------------------------|----------------|--------------------|--------------------|--------------------|--------------------|---------------------|--------------------|-----------------|--------------------|
```
The schema is completely flattened. There are no structs or arrays.
```
root
 |-- boolean_key: string (nullable = true)
 |-- empty_string_translation: string (nullable = true)
 |-- key_with_description: string (nullable = true)
 |-- key_with_lineminus__break: string (nullable = true)
 |-- null_translation: string (nullable = true)
 |-- simple_key: string (nullable = true)
 |-- unverified_key: string (nullable = true)
 |-- nested__key: string (nullable = true)
 |-- pluralized_key__one: string (nullable = true)
 |-- pluralized_key__other: string (nullable = true)
 |-- pluralized_key__zero: string (nullable = true)
 |-- sample_collection: string (nullable = true)
 |-- nested__deeply__key: string (nullable = true)
```
#### Notes

The nesting levels are denoted by double underscores `levelA__levelB__levelC`.
This is to prevent collisions with field names that have underscores naturally.
For example, in this scenario `nested_key` has an underscore in its name naturally,
whereas `nested__key` has been flattened.
```
|-- nested_key: string (nullable = true)
|-- nested__key: string (nullable = true)
```