


-- 4.  **Top 2 Project-Heavy Employees:** In **each department**, find the **Top 2 employees** assigned to the most projects.
-- List the employee name, department name, and their project count.

with top as
(
select e.name, d.dept_name,
count(ep.project_id) as proj_ct
from employees e
join departments d on d.dept_id = e.dept_id
join employee_project_assignments ep
on e.emp_id = ep.emp_id
join projects p
on p.project_id = ep.project_id
group by ep.emp_id,e.name, d.dept_name
),
rk as
(
select name,dept_name,proj_ct,
DENSE_RANK() OVER(PARTITION BY dept_name ORDER BY proj_ct DESC) AS rr
from top
) 
select name,dept_name,proj_ct
from rk
order by dept_name, rr
;
