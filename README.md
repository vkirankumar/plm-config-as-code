# plm-transform
Liquibase node app for plm catalog as config as code

#liquibase diffChangeLog --diffTypes=data,table,column,primaryKey,index,foreignKey,uniqueConstraint  --changelogFile=./change-diff.yaml
#liquibase diff --diff-types=data,column,table
#liquibase update --changelogFile=./change-diff.yaml
#liquibase tag-exists --tag=v1.0.0
#liquibase generateChangeLog --diffTypes=data,table,column,primaryKey,index,foreignKey,uniqueConstraint  --changelogFile=./change-log-master.yaml