-- ============================================================================
--  DDL: справочник типов IoT-устройств
--  (запускается автоматически при первом старте контейнера postgres)
-- ============================================================================

DROP TABLE IF EXISTS device_types;

CREATE TABLE device_types (
    id        INTEGER      PRIMARY KEY,   -- Id, идентификатор типа
    type_name VARCHAR(100) NOT NULL       -- TypeName, наименование типа
);

COMMENT ON TABLE  device_types            IS 'Статический справочник типов IoT-устройств';
COMMENT ON COLUMN device_types.id         IS 'Идентификатор типа устройства';
COMMENT ON COLUMN device_types.type_name  IS 'Наименование типа устройства';
