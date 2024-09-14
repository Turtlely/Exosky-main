# Job Manager

## Purpose

The purpose of these scripts is to allocate resources to tasks, in order to ensure that all jobs are completed.

## How it works

When a job is recieved, a job ID is automatically generated and sent to the client.

At the same time, this job is placed into a database, marking it for completion.

Another script reads goes through each item in the database and reads the parameters of each job.

These parameters are used to run a workflow, ultimately returning some amount of data.

This data is then placed into a pile of results, tagged with the job ID.

When the client requests data through their specific websocket using their job ID, we send them all of their data from the results pile. We then delete the result from the server end.

If the client fails to retrieve data, so that a certain amount of data builds up within their queue, the job is terminated.