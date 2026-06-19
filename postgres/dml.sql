-- ============================================================================
--  DML: наполнение справочника типов IoT-устройств
--  id здесь ДОЛЖНЫ совпадать с type_id, которые генерит generator/generator.py
-- ============================================================================

INSERT INTO device_types (id, type_name) VALUES
    (1, 'Thermostat'),
    (2, 'Humidity Sensor'),
    (3, 'HVAC Controller'),
    (4, 'Smart Plug'),
    (5, 'Air Quality Monitor')
ON CONFLICT (id) DO UPDATE
    SET type_name = EXCLUDED.type_name;
