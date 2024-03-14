# Databricks notebook source
# MAGIC %md
# MAGIC 	1) Create a github account (generalized one and personal one)
# MAGIC 	2) Create a git Repo in the generalized account
# MAGIC 	3) In databricks, create a connection with GIT by navigating to user setting add GIT email, GIT personal token.
# MAGIC 	4) Install Git on Databricks cluster (once only- already done)
# MAGIC        %sh apt-get update && apt-get install -y git
# MAGIC 	5) Navigate to Repos, add your repo.
# MAGIC 	6) Add collaborators in GitHub Repo so that everyone can access the same git Repo
# MAGIC 	7) Create branches and commit the changes
# MAGIC 	8) Create a pull request so that it can merge changes to the main branch
# MAGIC 	9) Review changes and approve
# MAGIC 	10) We can also delete the branch
# MAGIC     11) Go to databricks Repo, click on main branch and click pull to reflect all the changes made in main branch.