create table dim_clients
(
	client_id int not null,
	client_name varchar(128) not null
)
with (appendoptimized=true, orientation=column, compresstype=zlib, compresslevel=5)
distributed by (client_id)
;