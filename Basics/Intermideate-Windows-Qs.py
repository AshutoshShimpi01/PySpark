

That's a great PySpark window function problem\! The code you provided correctly uses the `lag` function to find the salary immediately lower than the current employee's salary **within their department**, after ordering by salary.

Here are 10 intermediate PySpark window function problems similar to the one you provided, focusing on various functions, partitions, and ordering concepts.

-----

## 💡 10 Intermediate PySpark Window Function Problems

### **1. Rank the Employees by Salary**

For each department, **rank** employees based on their salary from highest to lowest. Display the employee name, salary, department name, and the salary rank. Use the `rank` function.

```python
# Function: rank()
# Partition: dept_id
# Order: salary DESC
```

### **2. Calculate the Difference from Department's Highest Salary**

For each employee, calculate the **difference** between their salary and the highest salary within their department. Display the employee name, salary, and the difference. (Hint: Use `max` as a window function).

```python
# Function: max()
# Partition: dept_id
# Order: N/A (or a simple constant)
```

### **3. Cumulative Salary Total within Department**

Calculate the **running total (cumulative sum)** of salaries for employees within each department, ordered by their hire date.

```python
# Function: sum()
# Partition: dept_id
# Order: hire_date ASC
# Frame: rowsBetween(Window.unboundedPreceding, Window.currentRow)
```

### **4. Moving Average (3-Employee Window)**

Calculate the **3-employee moving average** of salary for each employee within their department. The average should include the current employee's salary, the salary immediately before them, and the salary immediately after them, ordered by employee ID.

```python
# Function: avg()
# Partition: dept_id
# Order: emp_id ASC
# Frame: rowsBetween(-1, 1)
```

### **5. First Employee Hired in Each Department**

For every employee, display the **name** of the first employee hired in their respective department. (Hint: Use `first` on the employee name, ordered by hire date).

```python
# Function: first()
# Partition: dept_id
# Order: hire_date ASC
```

### **6. Percentage of Department's Total Salary**

For each employee, calculate their salary as a **percentage** of the total salary paid by their department. (Hint: Use `sum` as a window function and a simple arithmetic calculation).

```python
# Function: sum()
# Partition: dept_id
# Order: N/A
```

### **7. Second Highest Salary in Each Department**

Find the employee(s) who earn the **second highest salary** in each department. (Hint: Use `dense_rank` and filter the results where the rank is 2).

```python
# Function: dense_rank()
# Partition: dept_id
# Order: salary DESC
```

### **8. Salary Immediately Higher (Lead Function)**

For each employee, display the name and salary of the employee who earns the salary immediately **higher** than them within their department. Order the employees by salary for this comparison. (This is the complement of your original problem).

```python
# Function: lead()
# Partition: dept_id
# Order: salary ASC
```

### **9. Difference in Hire Date from Previous Hire**

For each department, calculate the **number of days** between the current employee's hire date and the hire date of the employee hired immediately before them within that department. (Hint: Use `lag` on the `hire_date` column).

```python
# Function: lag()
# Partition: dept_id
# Order: hire_date ASC
```

### **10. Quartiles of Employee Age**

Assign each employee to a **quartile** (1, 2, 3, or 4) based on their age within the company (ignoring departments).

```python
# Function: ntile(4)
# Partition: N/A
# Order: age ASC
```

-----

Would you like to try solving the **Moving Average (3-Employee Window)** problem (Number 4)? I can provide the basic PySpark imports and DataFrame structure if needed.
