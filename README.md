# DorfbraeuDiverses

## Customer sync

`insertClients.R` provides functionality

-   to read and compare customer data from the accounting and the CRM database
-   generate missing/to be updated customers for the accounting db
-   sends the necessary SQL statements to the accounting db

The accounting DB is automatically being backed up in the process. Only customers from CRM which have invoice as paymentMethod are being considered.
