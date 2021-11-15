create table fact_out_of_stock
(
	date date not null,
	product_id int not null
)
with (appendoptimized=true, orientation=column, compresstype=zlib, compresslevel=5)
distributed by (product_id)
;