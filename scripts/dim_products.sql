create table dim_products
(
	product_id int not null,
	product_name varchar(128) not null,
	aisle varchar(128) not null,
	department varchar(128) not null
)
with (appendoptimized=true, orientation=column, compresstype=zlib, compresslevel=5)
distributed by (product_id)
;