library(DBI)
library(duckdb)
library(RSQLite)
library(uuid)
library(dplyr)
library(tidyr)
library(stringr)
library(yaml)


# prep work ---------------------------------------------------------------

# read the config file
conf <- read_yaml("config.yaml")

# backup the accounting DB
system(paste("cp",
             paste0(conf$gc$path, conf$gc$file),
             paste0(conf$gc$path, conf$gc$backup, str_extract(conf$gc$file, ".*\\."),
                    format(Sys.time(), "%Y%m%d_%H%M%S"))))

# open the DB connections
# DuckDB for temporary loading the CRM clients table
crm_con <- dbConnect(duckdb())

# connect to the GnuCash DB (which is SQLite)
acc_con <- dbConnect(SQLite(), paste0(conf$gc$path, conf$gc$file))
acc_df <- dbGetQuery(acc_con, "SELECT * FROM customers")

# parsing the dump --------------------------------------------------------
# in order to create the relevant CREATE and INSERT statements

# find the newest dump
path_dump <- list.files(conf$at$path, full.names = TRUE) |>
  file.info() |>
  arrange(desc(mtime)) |>
  slice(1) |>
  rownames()

# read the database dump
dump <- readLines(path_dump)

# find the start and end lines for the create statement of the client table
start_create <- grep("CREATE TABLE \\`clients\` \\(", dump)
end_create <- grep("^$", dump)[grep("^$", dump) > start_create][1] - 1

create_clients <- paste(dump[start_create:end_create], collapse = "") |>
  str_remove_all("`") |>
  str_replace("ENGINE.*", ";") |>
  str_replace("clients", "'clients'") |>
  str_replace_all("int\\([0-9]+\\)", "int")

# find the start and end lines for the insert statement of the client table
start_insert <- grep("INSERT INTO \\`clients\\` \\(", dump)
end_insert <- grep("^$", dump)[grep("^$", dump) > start_insert][1] - 1

insert_clients <- paste(dump[start_insert:end_insert], collapse = "") |>
  str_remove_all("`")

# prepare data for insert into GnuCash -------------------------------------------------

# write data into DuckDB. This is only needed to retrieve a proper data frame structure
dbSendStatement(crm_con, create_clients)
dbSendStatement(crm_con, insert_clients)

# transform the table structure of the CRM to the one of GnuCash and adapt field types where necessary
# id gets a leading "1" in order to be able to always differentiate the origin in GnuCash
crm_df <- dbGetQuery(crm_con, "SELECT * FROM clients") |>
  mutate(name = if_else(nchar(company) > 0, company, paste(prename, lastname)),
         id = as.character(id + 100000),
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
  # create a UUID. GnuCash requires a GUID w/o dashes
  mutate(guid = UUIDfromName(namespace = UUIDgenerate(output = "uuid"), name = name) |>
           str_replace_all("-", "")) |>
  # bring it into the right column order
  select(guid, name, id, notes, active, discount_num, discount_denom, credit_num, credit_denom, currency, tax_override,
         addr_name, addr_addr1, addr_addr2, addr_addr3, addr_addr4, addr_phone, addr_fax, addr_email,
         shipaddr_name, shipaddr_addr1, shipaddr_addr2, shipaddr_addr3, shipaddr_addr4, shipaddr_phone, shipaddr_fax, shipaddr_email,
         terms, tax_included, taxtable) |>
  # some customers are of older origin and exist already in GnuCash. We need to manually modify the ID's and GUID's
  # of the respective records in order to match the ID's of the target DB
  mutate(id = if_else(id == "1", "000001", id),
         id = if_else(id == "100076", "000002", id),
         id = if_else(id == "100038", "000003", id),
         id = if_else(id == "100040", "000004", id),
         id = if_else(id == "100071", "000005", id),
         id = if_else(id == "100037", "000006", id),
         id = if_else(id == "100070", "000007", id),
         id = if_else(id == "100028", "000008", id),
         id = if_else(id == "100027", "000009", id),
         id = if_else(id == "100043", "000010", id),
         id = if_else(id == "100029", "000011", id),
         id = if_else(id == "100065", "000012", id),
         id = if_else(id == "100025", "000013", id),
         id = if_else(id == "100086", "000014", id)) |>
  # take existing GUIDs for existing customers
  left_join(acc_df |> select(acc.guid = guid, id),
            by = "id") |>
  mutate(guid = if_else(is.na(acc.guid), guid, acc.guid)) |>
  select(-acc.guid)

# build the final dataframes for DB insert and DB update
upd <- crm_df |>
  semi_join(acc_df, by = "id")

ins <- crm_df |>
  anti_join(acc_df, by = "id")

# update and insert into GnuCash ------------------------------------------
# Update
dbWriteTable(acc_con, "temp", upd)
upd_stmt <-  "UPDATE customers AS c
              SET	name = t.name,
              			notes = t.notes,
              			addr_name = t.addr_name,
              			addr_addr1 = t.addr_addr1,
              			addr_phone = t.addr_phone,
              			addr_email = t.addr_email,
              			shipaddr_addr1 = t.shipaddr_addr1
              FROM temp t
              WHERE c.id = t.id"
dbSendStatement(acc_con, upd_stmt)

# Insert
dbWriteTable(acc_con, "customers", ins, append = TRUE)

# cleanup
dbRemoveTable(acc_con, "temp")
dbDisconnect(crm_con)
dbDisconnect(acc_con)
