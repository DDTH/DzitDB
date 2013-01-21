DROP PROCEDURE IF EXISTS dzitdb_create_table;
DROP PROCEDURE IF EXISTS dzitdb_drop_table;
DROP PROCEDURE IF EXISTS dzitdb_add_field;
DROP PROCEDURE IF EXISTS dzitdb_remove_field;

DELIMITER $$
CREATE PROCEDURE dzitdb_add_field(IN table_name VARCHAR(60), IN col_type VARCHAR(16), IN has_index INT)
BEGIN
    CASE col_type
        WHEN 'int' THEN IF has_index=0 THEN SET @sql = CONCAT('ALTER TABLE ', table_name, ' ADD COLUMN value_int BIGINT;');
            ELSE SET @sql = CONCAT('ALTER TABLE ', table_name, ' ADD COLUMN value_int BIGINT, ADD INDEX (value_int);');
            END IF;
        WHEN 'real' THEN IF has_index=0 THEN SET @sql = CONCAT('ALTER TABLE ', table_name, ' ADD COLUMN value_real DOUBLE;');
            ELSE SET @sql = CONCAT('ALTER TABLE ', table_name, ' ADD COLUMN value_real DOUBLE, ADD INDEX (value_real);');
            END IF;
        WHEN 'money' THEN IF has_index=0 THEN SET @sql = CONCAT('ALTER TABLE ', table_name, ' ADD COLUMN value_money NUMERIC(20,3);');
            ELSE SET @sql = CONCAT('ALTER TABLE ', table_name, ' ADD COLUMN value_money NUMERIC(20,3), ADD INDEX (value_money);');
            END IF;
        WHEN 'datetime' THEN IF has_index=0 THEN SET @sql = CONCAT('ALTER TABLE ', table_name, ' ADD COLUMN value_datetime DATETIME;');
            ELSE SET @sql = CONCAT('ALTER TABLE ', table_name, ' ADD COLUMN value_datetime DATETIME, ADD INDEX (value_datetime);');
            END IF;
        WHEN 'short_string' THEN IF has_index=0 THEN SET @sql = CONCAT('ALTER TABLE ', table_name, ' ADD COLUMN value_short_string VARCHAR(64);');
            ELSE SET @sql = CONCAT('ALTER TABLE ', table_name, ' ADD COLUMN value_short_string VARCHAR(64), ADD INDEX (value_short_string);');
            END IF;
        WHEN 'long_string' THEN SET @sql = CONCAT('ALTER TABLE ', table_name, ' ADD COLUMN value_long_string MEDIUMTEXT;');
        WHEN 'binary' THEN SET @sql = CONCAT('ALTER TABLE ', table_name, ' ADD COLUMN value_binary MEDIUMBLOB;');
    END CASE;
    PREPARE psqlStm from @sql;
    EXECUTE psqlStm;
END $$
DELIMITER ;

DELIMITER $$
CREATE PROCEDURE dzitdb_remove_field(IN table_name VARCHAR(60), IN col_type VARCHAR(16))
BEGIN
    CASE col_type
        WHEN 'int'          THEN SET @sql = CONCAT('ALTER TABLE ', table_name, ' DROP COLUMN value_int;');
        WHEN 'real'         THEN SET @sql = CONCAT('ALTER TABLE ', table_name, ' DROP COLUMN value_real;');
        WHEN 'money'        THEN SET @sql = CONCAT('ALTER TABLE ', table_name, ' DROP COLUMN value_money;');
        WHEN 'datetime'     THEN SET @sql = CONCAT('ALTER TABLE ', table_name, ' DROP COLUMN value_datetime;');
        WHEN 'short_string' THEN SET @sql = CONCAT('ALTER TABLE ', table_name, ' DROP COLUMN value_short_string;');
        WHEN 'long_string'  THEN SET @sql = CONCAT('ALTER TABLE ', table_name, ' DROP COLUMN value_long_string;');
        WHEN 'binary'       THEN SET @sql = CONCAT('ALTER TABLE ', table_name, ' DROP COLUMN value_binary;');
    END CASE;
    PREPARE psqlStm from @sql;
    EXECUTE psqlStm;
END $$
DELIMITER ;

DELIMITER $$
CREATE PROCEDURE dzitdb_create_table(IN table_name VARCHAR(60))
BEGIN
	SET @sql = CONCAT('DROP TABLE IF EXISTS ', table_name, ';');
	PREPARE psqlStm from @sql;
	EXECUTE psqlStm;
	
	SET @sql = CONCAT('CREATE TABLE ', 
	    table_name, '(',
	    'eid                   VARCHAR(32),',
	    'ekey                  VARCHAR(32),',
	    'esubkey               VARCHAR(32) DEFAULT "",',
	    'etype                 TINYINT     DEFAULT 0,',
	    'PRIMARY KEY (eid, ekey, esubkey)) ENGINE=InnoDB DEFAULT CHARACTER SET utf8 COLLATE utf8_unicode_ci;');
	PREPARE psqlStm from @sql;
    EXECUTE psqlStm;
END $$
DELIMITER ;

DELIMITER $$
CREATE PROCEDURE dzitdb_drop_table(IN table_name VARCHAR(60))
BEGIN
    SET @sql = CONCAT('DROP TABLE IF EXISTS ', table_name, ';');
    PREPARE psqlStm from @sql;
    EXECUTE psqlStm;
END $$
DELIMITER ;

DROP TABLE IF EXISTS dzitdb_info;
CREATE TABLE dzitdb_info (
    dbschema                    VARCHAR(32),
    dbtable                     VARCHAR(32),
    dbtable_info                TEXT,
    PRIMARY KEY (dbschema, dbtable)
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8 COLLATE utf8_unicode_ci;
