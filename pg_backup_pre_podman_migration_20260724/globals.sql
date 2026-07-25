--
-- PostgreSQL database cluster dump
--

\restrict G8CGjJbTR5nucrnVD5adwa7cPwzZKNSBp9EE7oLdaxq6KyeguoIcs5EksIJfg5v

SET default_transaction_read_only = off;

SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;

--
-- Roles
--

CREATE ROLE sourcing;
ALTER ROLE sourcing WITH SUPERUSER INHERIT CREATEROLE CREATEDB LOGIN REPLICATION BYPASSRLS;

--
-- User Configurations
--








\unrestrict G8CGjJbTR5nucrnVD5adwa7cPwzZKNSBp9EE7oLdaxq6KyeguoIcs5EksIJfg5v

--
-- PostgreSQL database cluster dump complete
--

