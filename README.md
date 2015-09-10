## canvara datasource
Datasource module for canvara

---
The data is stored in amazon dynamodb nosql datastore. This module is a wrapper around aws javascript sdk, with some common functionalities
included. For ex: validation, CRUD operations on dynamodb table etc

There will be one dynamodb instance per aws account id. The general anology with RDS databases is defined below

With RDS databases

```[hostname > ] database > schema > table```

With dynamodb

```[aws-region > aws-account-id > ] table```