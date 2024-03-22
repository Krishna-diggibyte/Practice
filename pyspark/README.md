# PySpark

This directory is dedicated to PySpark. Within this directory, I've honed my skills using various functions provided by PySpark.

## Common RDD Operations
## **Transformation**





### 1. `map()`
- Apply a function to each element of the RDD and return a new RDD with the results.

### 2. `flatMap()`
- Similar to `map()`, but flattens the result.

### 3. `filter()`
- Filter elements from the RDD based on a function.

### 4. `mapPartitions()`
- Apply a function to each partition of the RDD.

### 5. `mapPartitionsWithIndex()`
- Similar to `mapPartitions()`, but provides the index of each partition.

### 6. `glom()`
- Convert each partition of the RDD into a list.

### 7. `Union()`
- Combine two RDDs into one RDD.

### 8. `intersection()`
- Return the intersection of two RDDs.

### 9. `distinct()`
- Return distinct elements from the RDD.

### 10. `groupByKey()`
- Group the values for each key in the RDD.

## **Action**

Explore more functionalities with these additional RDD operations:

### 11. `collect()`
- Action that retrieves all elements of the RDD and returns them as an array in the driver program.
  
### 12. `take(n)`
- Action that returns the first `n` elements of the RDD as an array.

### 13. `top(n)`
- Action that returns the top `n` elements of the RDD based on the defined ordering.

### 14. `count()`
- Action that returns the number of elements in the RDD.

### 15. `min()`
- Action that returns the minimum element in the RDD.

### 16. `max()`
- Action that returns the maximum element in the RDD.

### 17. `mean()`
- Action that calculates the mean (average) value of the elements in the RDD.

### 18. `reduce(func)`
- Action that aggregates the elements of the RDD using a function `func`.

### 19. `countByKey()`
- Action applicable to pair RDDs. Counts the occurrences of each key in the RDD.

### 20. `countByValue()`
- Action that counts the occurrences of each unique value in the RDD.

### 21. `fold(zeroValue, func)`
- Action similar to `reduce()`, but requires a zero value of the same type as the RDD elements.

### 22. `range(start, end, step)`
- Action that creates an RDD with a range of numbers from `start` to `end` (exclusive) with a given step size.
