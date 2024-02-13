<?php
    /**
     * Definicion de constantes.
     */

    // Si debe mostrar los mensajes por consola.
    define('__DB_MIRROR_SHOW_MESSAGES', true);

    // Nombre de la tabla de configuracion.
    define('__DB_MIRROR_CONFIG_TABLENAME', '__db_mirror_config__');

    // Nombre del archivo de registro de eventos.
    define('__DB_MIRROR_EVENT_FILENAME', 'dbmirror.log');
    
    /**
     * Muestra un mensaje por consola.
     */
    function showMessage($message, $endOfLine = PHP_EOL) {
        if (__DB_MIRROR_SHOW_MESSAGES) {
            echo $message . $endOfLine;
        }
    }

    /**
     * Muestra la informacion del programa.
     */
    function showProgramInfo() {
        $message =
            "DB-Mirror Herramienta de Replicación v.4.2.05" . PHP_EOL .
            "Dev. Oscar E. Urdaneta - 2023" . PHP_EOL;
        showMessage($message);
    }

    /**
     * Muestra la ayuda de los parametros.
     */
    function showHelp() {
        $message =
            "   Modo de uso:" . PHP_EOL .
            "   php dbmirror.php [parametros]" . PHP_EOL .
            PHP_EOL .
            "   Parametros:" . PHP_EOL .
            "      -h o --help                Muestra esta ayuda" . PHP_EOL .
            "      -t o --table <table_name>  Procesa solo la tabla especificada (tiene precedencia sobre -a)" . PHP_EOL .
            "      -a o --all                 Omite la reanudación, procesa todas las tablas" . PHP_EOL .
            "      -c o --create              Obliga a crear las tablas procesadas en el destino" . PHP_EOL .
            "      -r o --remove-id-field     Elimina el campo __db_mirror_id__ de la base de datos origen";
        showMessage($message);
    }

    /**
     * Genera el comando para crear una tabla MySQL a partir de un arreglo
     * asociativo con la estructura de la tabla SQL Server.
     */
    function getCreateTableCommand($tableName, $structure) {
        $command = "create table $tableName (
            __db_mirror_id__ int(10) not null,
            __md5__ varchar(32) not null";

        for ($i = 0; $i < count($structure); $i++) {
            $fieldName = strtolower($structure[$i]['columnname']);

            if ($fieldName == '__db_mirror_id__') {
                continue;
            }

            $typeId = intval($structure[$i]['system_type_id']);
            $length = intval($structure[$i]['max_length']);
            $precision = intval($structure[$i]['scale']);

            $command .= ", ";

            switch ($typeId) {
                // Tipo image (blob).
                case 34:
                    $command .= "$fieldName blob not null";
                    break;
                // Tipo date.
                case 40:
                    $command .= "$fieldName date not null";
                    break;
                // Tipos: tinyint, smallint, int.
                case 48:
                case 52:
                case 56:
                    $command .= "$fieldName int($length) not null";
                    break;
                // Tipo: datetime.
                case 61:
                    $command .= "$fieldName datetime not null";
                    break;
                // Tipo: decimal y numeric.
                case 106:
                case 108:
                    $command .= "$fieldName decimal($length, $precision) not null";
                    break;
                // Tipo char.
                case 175:
                    $command .= "$fieldName char($length) not null";
                    break;
                // Tipo: varchar.
                case 167:
                    $command .= "$fieldName varchar($length) not null";
                    break;
                // Tipo: text, nvarchar.
                case 35:
                case 231:
                case 241:
                    $command .= "$fieldName text not null";
                    break;
                // Tipo binary.
                case 173:
                    $command .= "$fieldName binary not null";
                    break;
                default:
                    saveLog("Tipo de campo desconocido: $typeId");
                    return false;
            }

            $commaFlag = true;
        }

        $command .= ", primary key (__db_mirror_id__)) engine=InnoDb default charset=utf8 collate=utf8_unicode_ci;";
        return $command;
    }

    /**
     * Devuelve una lista de campos validos para la consulta.
     */
    function getValidFields($structure) {
        $fieldList = '';

        for ($k = 0; $k < count($structure); $k++) {
            $fieldName = $structure[$k]['columnname'];

            // Si es el __db_mirror_id__ lo omite.
            if ($fieldName == '__db_mirror_id__') {
                continue;
            }

            $typeId = intval($structure[$k]['system_type_id']);

            // Si el tipo es xml lo omite.
            if ($typeId == 241) {
                continue;
            }

            if (strlen($fieldList) > 0) {
                $fieldList .= ', ';
            }
            
            $fieldList .= $fieldName;
        }

        return $fieldList;
    }

    /**
     * Devuelve los campos y los valores para una actualizacion.
     */
    function getFieldsAndValues($structure, $record) {
        $valueList = '';
        for ($k = 0; $k < count($structure); $k++) {
            $fieldName = $structure[$k]['columnname'];

            // Si es el __db_mirror_id__ lo omite.
            if ($fieldName == '__db_mirror_id__') {
                continue;
            }

            $typeId = intval($structure[$k]['system_type_id']);

            // Si el tipo es xml lo omite.
            if ($typeId == 241) {
                continue;
            }

            if (strlen($valueList) != 0) {
                $valueList .= ', ';
            }

            // Si es un tipo DateTime.
            if (is_a($record[$fieldName], 'DateTime')) {
                $record[$fieldName] = date_format($record[$fieldName], 'Y-m-d');
            }

            $value = str_replace("'", ' ', $record[$fieldName]);
            $valueList .= "$fieldName = '" . $value . "'";
        }

        return $valueList;
    }

    /**
     * Devuelve los valores de un registro omitiendo los campos xml.
     */
    function getValidValues($structure, $record) {
        $valueList = '';
        for ($k = 0; $k < count($structure); $k++) {
            $fieldName = $structure[$k]['columnname'];

            // Si es el __db_mirror_id__ lo omite.
            if ($fieldName == '__db_mirror_id__') {
                continue;
            }

            $typeId = intval($structure[$k]['system_type_id']);

            // Si el tipo es xml lo omite.
            if ($typeId == 241) {
                continue;
            }

            if (strlen($valueList) > 0) {
                $valueList .= ', ';
            }

            // Si no es una cadena lo convierte.
            if (is_a($record[$fieldName], 'DateTime')) {
                $d = $record[$fieldName];
                $value = $d->format('Y-m-d');
            } else {
                $value = $record[$fieldName];
            }

            $value = str_replace("'", ' ', $value);
            $valueList .= "'$value'";
        }

        return $valueList;
    }

    /**
     * Guarda un texto en el log.
     */
    function saveLog($log) {
        $filename = __DB_MIRROR_EVENT_FILENAME;
        $file = fopen(__DIR__ . "/$filename", "a");
        fwrite($file, $log . PHP_EOL);
        fclose($file);
    }
?>
