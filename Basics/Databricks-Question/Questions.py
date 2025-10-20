These 20 questions are designed to test your understanding of intermediate PySpark techniques, including complex joins, window functions, and advanced data manipulation, all using the provided schema.

## 20 PySpark Interview/Practice Questions

### Window Functions (Lag, Lead, Rank, Sum, Avg)

1.  **Top N Per Group (Dense Rank):** Find the top 2 highest-paid employees in each department and display their name, department name, and salary. Use the `dense_rank()` window function.
2.  **Relative Salary Comparison (Avg):** Calculate a new column showing the difference between each employee's salary and the average salary of their respective department.
3.  **Value Comparison (Lag/Lead):** For each employee, display the name and salary of the employee who earns the salary immediately **lower** than them within their department. Order the employees by salary for this comparison.
4.  **Running Total:** Calculate the running total of salaries across the entire company, ordering the accumulation based on individual employee salary (from lowest to highest).
5.  **Department Total and Percentage:** Add two columns: one showing the total salary expenditure for the employee's department, and another showing the employee's salary as a percentage of that department total.
6.  **Three-Row Moving Average:** For each employee, calculate the **average salary** of the current employee, the previous employee, and the next employee within their department (ordering by `emp_id`).
7.  **Rank Gaps (Rank vs. Dense Rank):** Compare the results of using `rank()` and `dense_rank()` when ranking employees by salary within a department, specifically to illustrate the difference in how ties are handled.
8.  **First Value in Partition:** For every employee, display the salary of the **highest-paid employee** in their respective department on the same row, using the `first()` window function and ordering by salary descending.

### Joins and Filtering

9.  **Missing Project Assignments (Anti-Join):** List the names of all employees who are **not** currently assigned to any project (i.e., their `emp_id` does not appear in `projects_df`).
10. **Unmatched Departments (Full Join):** Perform a full outer join between `employees_df` and `departments_df` to show all employees, all departments, and identify any departments that have no employees (like 'Sales' in the data).
11. **Orphan Projects (Right Join):** Identify and list the `project_name`s that reference an `emp_id` that does not exist in `employees_df` (the orphan `emp_id` 11 in the data).
12. **Filtering on Total (Aggregation & Subquery/Window):** Find the name and salary of employees who belong to a department where the **average salary** is greater than \$60,000.
13. **Multi-Step Join:** Find the `dept_name` and `project_name` for all employees who earn more than \$65,000.

### Aggregation and Data Manipulation

14. **Top N Aggregation:** Identify the department that has the highest **total salary expenditure** and display only that department's name and total salary.
15. **Unique Projects per Department:** List all department names and the count of unique projects that employees in that department are associated with.
16. **Conditional Column (`when/otherwise`):** Add a column called `Salary_Level` to `employees_df` based on the following rules: Salary $\le 55000$ = 'Junior', Salary $> 55000$ AND Salary $\le 65000$ = 'Mid-Level', Salary $> 65000$ = 'Senior'.
17. **Grouping on Derived Field:** Group employees by their first letter of their name and count how many employees fall into each group.
18. **Highest Project Count:** Determine which employee has been assigned to the maximum number of projects and list their name and the total project count.
19. **Filtering with `LIKE`:** List the name and department name of all employees whose name contains the letter 'a' (case-insensitive).
20. **Pivot/Transpose Data:** Transform the employee data to show `dept_id` as rows and aggregated metrics (e.g., `max_salary`, `min_salary`, `avg_salary`) as columns, using the `pivot` function.
