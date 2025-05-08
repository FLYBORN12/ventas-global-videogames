CREATE TABLE public.tbl_dimension_editorial (
	id_edito serial4 NOT NULL,
	nombre varchar(100) NOT NULL,
	CONSTRAINT tbl_dimension_editorial_pkey PRIMARY KEY (id_edito),
	CONSTRAINT unique_nombre_edito UNIQUE (nombre)
);

CREATE TABLE public.tbl_dimension_plataforma (
	id_plata serial4 NOT NULL,
	nombre varchar(100) NOT NULL,
	CONSTRAINT tbl_dimension_plataforma_pkey PRIMARY KEY (id_plata),
	CONSTRAINT unique_nombre UNIQUE (nombre)
);

CREATE TABLE public.tbl_dimension_tiempo (
	id_tiempo serial4 NOT NULL,
	anio int4 NOT NULL,
	CONSTRAINT tbl_dimension_tiempo_pkey PRIMARY KEY (id_tiempo),
	CONSTRAINT unique_anio UNIQUE (anio)
);

CREATE TABLE public.tbl_dimension_game (
	id_game serial4 NOT NULL,
	nombre varchar(100) NOT NULL,
	id_tiempo_lanzamiento int4 NULL,
	CONSTRAINT tbl_dimension_game_pkey PRIMARY KEY (id_game),
	CONSTRAINT unique_name UNIQUE (nombre),
	CONSTRAINT tbl_dimension_game_id_tiempo_lanzamiento_fkey FOREIGN KEY (id_tiempo_lanzamiento) REFERENCES public.tbl_dimension_tiempo(id_tiempo)
);

CREATE TABLE public.ventas (
	id_ventas serial4 NOT NULL,
	id_game int4 NOT NULL,
	id_edito int4 NOT NULL,
	id_plata int4 NOT NULL,
	id_tiempo int4 NOT NULL,
	valor_venta numeric(12, 2) NOT NULL,
	CONSTRAINT ventas_pkey PRIMARY KEY (id_ventas),
	CONSTRAINT ventas_id_edito_fkey FOREIGN KEY (id_edito) REFERENCES public.tbl_dimension_editorial(id_edito),
	CONSTRAINT ventas_id_game_fkey FOREIGN KEY (id_game) REFERENCES public.tbl_dimension_game(id_game),
	CONSTRAINT ventas_id_plata_fkey FOREIGN KEY (id_plata) REFERENCES public.tbl_dimension_plataforma(id_plata),
	CONSTRAINT ventas_id_tiempo_fkey FOREIGN KEY (id_tiempo) REFERENCES public.tbl_dimension_tiempo(id_tiempo)
);
