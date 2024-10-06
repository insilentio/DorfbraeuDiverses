library(DBI)
library(duckdb)
library(RSQLite)
library(uuid)
library(dplyr)
library(tidyr)
library(stringr)


# parsing the dump in order to create the relevant CREATE and INSERST statements --------

# read the database dump
dump <- readLines("/Users/Daniel/Dorfbräu/Dokumente/AdminTool/quasitut_adminDorfbrau_dbBackup_2024_09_16.sql")

# find the start and end lines for the create statement of the client table
start_create <- grep("CREATE TABLE \\`clients\` \\(", dump)
end_create <- grep("^$", dump)[grep("^$", dump) > start_create][1]-1

create_clients <- paste(dump[start_create:end_create], collapse = "") |> 
  str_remove_all("`") |> 
  str_replace("ENGINE.*", ";") |> 
  str_replace("clients", "'clients'") |> 
  str_replace_all("int\\([0-9]+\\)", "int")

# find the start and end lines for the insert statement of the client table
start_insert <- grep("INSERT INTO \\`clients\\` \\(", dump)
end_insert <- grep("^$", dump)[grep("^$", dump) > start_insert][1]-1

insert_clients <- paste(dump[start_insert:end_insert], collapse = "") |> 
  str_remove_all("`")

# prepare data for insert into GnuCash -------------------------------------------------

# write data into DuckDB. This is only needed to retrieve a proper data frame structure
thom_con <- dbConnect(duckdb())
dbSendStatement(thom_con, create_clients)
dbSendStatement(thom_con, insert_clients)

# transform the table structure of the CRM to the one of GnuCash
mydf <- dbGetQuery(thom_con, "SELECT * FROM clients") |>
  mutate(name = if_else(nchar(company) > 0, company, paste(prename, lastname)),
         active = 1,
         discount_num = 0,
         discount_denom = 1,
         credit_num = 0,
         credit_denom = 1,
         currency = "4860173d4e644de98bcb4e491e465ff5",
         tax_override = 0,
         addr_name = paste(prename, lastname),
         addr_addr1 = address,
         addr_addr2 = "",
         addr_addr3 = "",
         addr_addr4 = "",
         addr_phone = phone,
         addr_fax = "",
         addr_email = email,
         shipaddr_name = "",
         shipaddr_addr1 = billingAddress,
         shipaddr_addr2 = "",
         shipaddr_addr3 = "",
         shipaddr_addr4 = "",
         shipaddr_phone = "",
         shipaddr_fax = "",
         shipaddr_email = "",
         terms = "356a93547672413c8795b3d0b22701bf",
         tax_included = 3,
         taxtable = "") |> 
  mutate(guid = UUIDfromName(namespace = UUIDgenerate(), name = name)) |> 
  select(guid, name, id, notes, active, discount_num, discount_denom, credit_num, credit_denom, currency, tax_override,
         addr_name, addr_addr1, addr_addr2, addr_addr3, addr_addr4, addr_phone, addr_fax, addr_email,
         shipaddr_name, shipaddr_addr1, shipaddr_addr2, shipaddr_addr3, shipaddr_addr4, shipaddr_phone, shipaddr_fax, shipaddr_email,
         terms, tax_included, taxtable)



# connect to the GnuCash DB (which is SQLite)
my_con <- dbConnect(SQLite(), "/Volumes/NoBackup/Downloads/Test2Buchhaltung.gnucash")
acc_df <- dbGetQuery(my_con, "SELECT * FROM customers")




dbDisconnect(thom_con)
dbDisconnect(db_con)
