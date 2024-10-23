library(DBI)
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

# connect to the GnuCash DB (which is SQLite)
acc_con <- dbConnect(SQLite(), paste0(conf$gc$path, conf$gc$file))
acc_df <- dbGetQuery(acc_con, "SELECT * FROM customers")

# connect to CRM database
crm_con <- dbConnect(
  odbc::odbc(),
  driver = "MariaDB",
  database = conf$at$db,
  user = conf$at$user,
  password = conf$at$pw,
  server   = conf$at$server,
  port   = conf$at$port
)
clients <- dbGetQuery(crm_con, "SELECT * FROM clients")
orders <- dbGetQuery(crm_con,  
                     "SELECT id AS orderId, clientId, paymentMethod
                      FROM orders
                      WHERE paymentMethod = 1")

# prepare data for insert into GnuCash -------------------------------------------------

# transform the table structure of the CRM to the one of GnuCash and adapt field types where necessary
# id gets a leading "1" in order to be able to always differentiate the origin in GnuCash
crm_df <- orders |> 
  distinct(clientId) |> 
  left_join(clients, by = join_by(clientId == id)) |> 
  mutate(name = if_else(nchar(company) > 0, company, paste(prename, lastname)),
         id = as.character(clientId + 100000),
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
  mutate(id = if_else(id == "100076", "000002", id),
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
         id = if_else(id == "100080", "000013", id),
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
