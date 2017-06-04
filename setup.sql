--
-- PostgreSQL database dump
--

SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;

--
-- Name: plpgsql; Type: EXTENSION; Schema: -; Owner: 
--

CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;


--
-- Name: EXTENSION plpgsql; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';


SET search_path = public, pg_catalog;

--
-- Name: disburse_mapping(); Type: FUNCTION; Schema: public; Owner: mark
--

CREATE FUNCTION disburse_mapping() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
  rec RECORD;
  masterid integer;
begin
  SELECT INTO rec * from table_map where sobject_name = NEW.sobject_name ;
  IF NOT FOUND THEN
    INSERT INTO table_map (sobject_name, table_name) values (NEW.sobject_name, NEW.table_name);
    SELECT INTO rec * from table_map where sobject_name = NEW.sobject_name;
    masterid := rec.id;
  ELSE
    masterid := rec.id;
    SELECT INTO rec * from field_map where table_map = masterid and sobject_field = NEW.sobject_field;
    IF FOUND THEN
        return NEW;
    END IF;
  END IF;

  INSERT INTO field_map (sobject_field,db_field,table_map,fieldtype) values (NEW.sobject_field,NEW.table_field,masterid,NEW.fieldtype);
  return NEW;
end $$;


ALTER FUNCTION public.disburse_mapping() OWNER TO mark;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: field_map; Type: TABLE; Schema: public; Owner: mark; Tablespace: 
--

CREATE TABLE field_map (
    sobject_field character varying(200) NOT NULL,
    db_field character varying(200) NOT NULL,
    table_map integer NOT NULL,
    fieldtype character varying(50) NOT NULL
);


ALTER TABLE public.field_map OWNER TO mark;

--
-- Name: map_drop; Type: TABLE; Schema: public; Owner: mark; Tablespace: 
--

CREATE TABLE map_drop (
    sobject_name character varying(200) NOT NULL,
    table_name character varying(200) NOT NULL,
    sobject_field character varying(200) NOT NULL,
    table_field character varying(200) NOT NULL,
    fieldtype character varying(50) NOT NULL
);


ALTER TABLE public.map_drop OWNER TO mark;

--
-- Name: table_map; Type: TABLE; Schema: public; Owner: mark; Tablespace: 
--

CREATE TABLE table_map (
    id integer NOT NULL,
    sobject_name character varying(200) NOT NULL,
    table_name character varying(200) NOT NULL
);


ALTER TABLE public.table_map OWNER TO mark;

--
-- Name: table_map_id_seq; Type: SEQUENCE; Schema: public; Owner: mark
--

CREATE SEQUENCE table_map_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.table_map_id_seq OWNER TO mark;

--
-- Name: table_map_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: mark
--

ALTER SEQUENCE table_map_id_seq OWNED BY table_map.id;


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: mark
--

ALTER TABLE ONLY table_map ALTER COLUMN id SET DEFAULT nextval('table_map_id_seq'::regclass);


--
-- Name: table_map_pkey; Type: CONSTRAINT; Schema: public; Owner: mark; Tablespace: 
--

ALTER TABLE ONLY table_map
    ADD CONSTRAINT table_map_pkey PRIMARY KEY (id);


--
-- Name: fki_table_map; Type: INDEX; Schema: public; Owner: mark; Tablespace: 
--

CREATE INDEX fki_table_map ON field_map USING btree (table_map);


--
-- Name: disburse_mapping; Type: TRIGGER; Schema: public; Owner: mark
--

CREATE TRIGGER disburse_mapping BEFORE INSERT ON map_drop FOR EACH ROW EXECUTE PROCEDURE disburse_mapping();


--
-- Name: table_map; Type: FK CONSTRAINT; Schema: public; Owner: mark
--

ALTER TABLE ONLY field_map
    ADD CONSTRAINT table_map FOREIGN KEY (table_map) REFERENCES table_map(id);


--
-- Name: public; Type: ACL; Schema: -; Owner: mark
--

REVOKE ALL ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM mark;
GRANT ALL ON SCHEMA public TO mark;
GRANT ALL ON SCHEMA public TO PUBLIC;


--
-- PostgreSQL database dump complete
--

