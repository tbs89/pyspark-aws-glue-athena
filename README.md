
<body>

<h1>STEDI Human Balance Analytics Project</h1>

<p>This project involves using AWS Glue, Spark, and AWS Athena to process and transform data from multiple sources, ultimately creating curated data for machine learning purposes. The STEDI Step Trainer collects sensor data to train a machine learning model for detecting steps and balance exercises.</p>

<h2>Project Structure</h2>
<pre>
project_root/
├── glue_jobs/
│   ├── customer_landing_to_trusted.py
│   ├── accelerometer_landing_to_trusted.py
│   ├── customer_curated.py
│   ├── step_trainer_landing_to_trusted.py
│   ├── machine_learning_curated.py
│
├── sql_scripts/
│   ├── customer_landing.sql
│   ├── accelerometer_landing.sql
│   ├── step_trainer_landing.sql
│
├── screenshots/
│   ├── customer_landing.png
│   ├── accelerometer_landing.png
│   ├── step_trainer_landing.png
│   ├── customer_trusted.png
│   ├── accelerometer_trusted.png
│   ├── customer_curated.png
│   ├── machine_learning_curated.png
│
├── README.md
</pre>

<h2>Glue Jobs</h2>
<ul>
    <li><code>customer_landing_to_trusted.py</code>: Transforms data from <code>customer_landing</code> to <code>customer_trusted</code>.</li>
    <li><code>accelerometer_landing_to_trusted.py</code>: Transforms data from <code>accelerometer_landing</code> to <code>accelerometer_trusted</code>.</li>
    <li><code>customer_curated.py</code>: Transforms data from <code>customer_trusted</code> and <code>accelerometer_trusted</code> to <code>customer_curated</code>.</li>
    <li><code>step_trainer_landing_to_trusted.py</code>: Transforms data from <code>step_trainer_landing</code> to <code>step_trainer_trusted</code>.</li>
    <li><code>machine_learning_curated.py</code>: Transforms data from <code>step_trainer_trusted</code> and <code>accelerometer_trusted</code> to <code>machine_learning_curated</code>.</li>
</ul>

<h2>SQL Scripts</h2>
<ul>
    <li><code>customer_landing.sql</code>: Creates the <code>customer_landing</code> table.</li>
    <li><code>accelerometer_landing.sql</code>: Creates the <code>accelerometer_landing</code> table.</li>
    <li><code>step_trainer_landing.sql</code>: Creates the <code>step_trainer_landing</code> table.</li>
</ul>

<h2>Screenshots</h2>
<ul>
    <li><code>customer_landing.png</code>: Screenshot of the query result for <code>customer_landing</code> in Athena.</li>
    <li><code>accelerometer_landing.png</code>: Screenshot of the query result for <code>accelerometer_landing</code> in Athena.</li>
    <li><code>step_trainer_landing.png</code>: Screenshot of the query result for <code>step_trainer_landing</code> in Athena.</li>
    <li><code>customer_trusted.png</code>: Screenshot of the query result for <code>customer_trusted</code> in Athena.</li>
    <li><code>accelerometer_trusted.png</code>: Screenshot of the query result for <code>accelerometer_trusted</code> in Athena.</li>
    <li><code>customer_curated.png</code>: Screenshot of the query result for <code>customer_curated</code> in Athena.</li>
    <li><code>machine_learning_curated.png</code>: Screenshot of the query result for <code>machine_learning_curated</code> in Athena.</li>
</ul>

</body>
</html>
