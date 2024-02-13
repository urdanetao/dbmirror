<?php
    /**
     * Programa: DbMirror
     * Replica las tablas de una base de datos SQL Server a otra MySql
     * Dev. Oscar E. Urdaneta
     * Date: 2023-05-08
     */

    // Archivos externos.
    include __DIR__ . '/dbmirror-lib.php';
    include __DIR__ . '/dbinfo.php';
    include __DIR__ . '/sql-server-data-manager.php';
    include __DIR__ . '/mysql-data-manager.php';

    /**
     * Constantes.
     */

    // Tamaño de los buferes de lectura.
    define('__max_buffer_size', 500);

    // Numero de maximo de registros a cargar por consulta.
    define('__max_records_by_query', 5000);

    // Muestra la informacion de la aplicacion.
    showProgramInfo();

    // Toma la hora de inicio.
    date_default_timezone_set("America/Bogota");
    $startTime = date("d-m-Y h:i:s");

    // Parametros desde la linea de comandos.
    $processAllTables = false;
    $createTable = false;
    $processTableFlag = false;
    $removeIdField = false;
    $processTableName = '';
    $pCount = count($argv);

    if ($pCount > 1) {
        for ($i = 1; $i < $pCount; $i++) {
            $param = $argv[$i];
            switch($param) {
                case '-h':
                case '--help':
                    if ($i == 1 && $pCount == 2) {
                        showHelp();
                    } else {
                        showMessage("El parametro '$param' debe usarse solo");
                        return 1;
                    }
                    return;
                case '-a':
                case '--all':
                    $processAllTables = true;
                    break;
                case '-c':
                case '--create':
                    $createTable = true;
                    break;
                case '-t':
                case '--table':
                    $processTableFlag = true;
                    break;
                case '-r':
                case '--remove-id-field':
                    $removeIdField = true;
                    break;
                default:
                    if ($processTableFlag) {
                        if ($processTableName == '') {
                            $processTableName = $param;
                        } else {
                            showMessage("La tabla a procesar ya fue establecida");
                            return 1;
                        }
                    } else {
                        showMessage("Parametro '$param' desconocido");
                        return 1;
                    }
                    break;
            }
        }

        // Si se establecio el parametro para remover el campo __db_mirror_id__.
        if ($removeIdField) {
            if ($processAllTables || $createTable) {
                showMessage("El parametro -r solo puede ser acompañado con el parametro -t");
                return 1;
            }
        }

        // Si se establecio el parametro para procesar una sola tabla.
        if ($processTableFlag) {
            // Si no se espeifico la tabla.
            if ($processTableName == '') {
                showMessage("Debe indicar la tablaa procesar");
                return 1;
            }

            // Si se indico el parametro para procesar todas las tablas.
            if ($processAllTables) {
                showMessage("Se ha indicado procesar una sola tabla, se omite el parametro -a");
            }
        }
    }

    // Muestra el tamaño del buffer de trabajo.
    showMessage('Buffer de trabajo: ' . strval(__max_buffer_size) . ' regs/ciclo');

    /**
     * Conecta con el servidor sql server. (origen)
     */
    $dbInfo = getSqlServerDbInfo();
    showMessage("Conectando con el servidor de origen origen => " . $dbInfo['host'] . " : " . $dbInfo['dbname']);
    $sqlServer = new SqlServerDataManager($dbInfo);

    if (!$sqlServer->IsConnected()) {
        showMessage($sqlServer->GetErrorMessage());
        return 1;
    }

    /**
     * Conecta con el servidor mysql. (destino)
     */
    $dbInfo = getMySqlDbInfo();
    showMessage("Conectando con el servidor de destino (Reader) => " . $dbInfo['host'] . " : " . $dbInfo['dbname']);
    $mySqlReader = new MySqlDataManager($dbInfo);

    if (!$mySqlReader->IsConnected()) {
        showMessage($mySqlReader->GetErrorMessage());
        $sqlServer->Close();
        return 1;
    }

    showMessage("Conectando con el servidor de destino (Writer) => " . $dbInfo['host'] . " : " . $dbInfo['dbname']);
    $mySqlWriter = new MySqlDataManager($dbInfo);

    if (!$mySqlWriter->IsConnected()) {
        showMessage($mySqlWriter->GetErrorMessage());
        $sqlServer->Close();
        $mySqlReader->Close();
        return 1;
    }

    // Valida la tabla de configuracion.
    showMessage("Validando la tabla de configuracion...");
    $configTableName = __DB_MIRROR_CONFIG_TABLENAME;
    $exist = $mySqlReader->ExistTable($configTableName);
    if (!$exist || $createTable) {
        // Si existe la elimina.
        if ($exist) {
            showMessage("Eliminando la tabla de configuración...");
            $sqlCommand = "drop table $configTableName";
            if (!$mySqlReader->Query($sqlCommand)) {
                showMessage($mySqlReader->GetErrorMessage());
                $sqlServer->Close();
                $mySqlReader->Close();
                $mySqlReader->Close();
                return 1;
            }
        }

        // Crea la tabla de configuracion.
        showMessage("Creando tabla de configuración...");
        $sqlCommand =
            "create table
                $configTableName (
                    id int(10) not null,
                    on_process varchar(30) not null,
                    lastupdate datetime not null,
                    primary key (id)
                ) engine=InnoDb default charset=utf8 collate=utf8_unicode_ci";
        if (!$mySqlReader->Query($sqlCommand)) {
            showMessage($mySqlReader->GetErrorMessage());
            $sqlServer->Close();
            $mySqlReader->Close();
            $mySqlReader->Close();
            return 1;
        }

        $sqlCommand = "insert into $configTableName (id, on_process) values (1, '')";
        if (!$mySqlReader->Query($sqlCommand)) {
            showMessage($mySqlReader->GetErrorMessage());
            $sqlServer->Close();
            $mySqlReader->Close();
            $mySqlReader->Close();
            return 1;
        }
        showMessage("Tabla de configuracion creada con exito...");
    } else {
        showMessage("Tabla de configuracion => ok...");
    }
    
    // Carga el registro de configuracion.
    showMessage("Cargando registro de configuración...");
    $sqlCommand = "select t.* from $configTableName as t where t.id = '1'";
    $config = $mySqlReader->Query($sqlCommand);

    // Si ocurrio un error en la consulta.
    if ($config === false) {
        showMessage($mySqlReader->GetErrorMessage());
        $sqlServer->Close();
        $mySqlReader->Close();
        $mySqlReader->Close();
        return 1;
    }

    // Si no existe el registro de configuracion.
    if (count($config) == 0) {
        $sqlCommand = "insert into $configTableName (id, on_process) values ('1', '')";
        if (!$mySqlReader->Query($sqlCommand)) {
            showMessage($mySqlReader->GetErrorMessage());
            $sqlServer->Close();
            $mySqlReader->Close();
            $mySqlReader->Close();
            return 1;
        }

        $sqlCommand = "select t.* from $configTableName as t where t.id = '1'";
        $config = $mySqlReader->Query($sqlCommand);
    
        // Si ocurrio un error en la consulta.
        if ($config === false) {
            showMessage($mySqlReader->GetErrorMessage());
            $sqlServer->Close();
            $mySqlReader->Close();
            $mySqlReader->Close();
            return 1;
        }
    }

    // Apunta al registro de configuracion.
    $config = $config[0];

    // Toma la lista de tablas de origen.
    showMessage("Cargando lista de tablas de origen...");
    $sourceTableList = $sqlServer->GetTables();

    // Toma la lista de tablas de destino.
    showMessage("Cargando lista de tablas de destino...");
    $targetTableList = $mySqlReader->GetTables();

    $tableFound = false;
    if ($processTableFlag) {
        $config['on_process'] = $processTableName;
        showMessage("Procesando solo la tabla '$processTableName'...");
    } else {
        $tableFound = $processAllTables;
    }

    // Busca la primera tabla a procesar.
    $i = 0;
    $totalSourceTables = count($sourceTableList);
    if ($config['on_process'] != '') {
        if ($processAllTables) {
            $config['on_process'] = '';
        } else {
            $found = false;
            while ($i < $totalSourceTables) {
                if ($sourceTableList[$i] == $config['on_process']) {
                    $found = true;
                    break;
                }
                $i++;
            }
    
            if (!$found) {
                showMessage("La tabla '" . $config['on_process'] . "' no existe...");
                $sqlServer->Close();
                $mySqlReader->Close();
                $mySqlReader->Close();
                return 1;
            }
        }
    }

    // Inicializa los contadores de registros procesados.
    $totalInserted = 0;
    $totalUpdated = 0;
    $totalDeleted = 0;
    $totalRecords = 0;

    // Ciclo principal de replicacion.
    while ($i < $totalSourceTables) {
        // Toma el nombre de la tabla.
        $tableName = $sourceTableList[$i];

        // Establece la tabla que se esta procesando.
        $sqlCommand = "update $configTableName set on_process = '$tableName' where id = '1'";
        if ($mySqlWriter->Query($sqlCommand) === false) {
            showMessage("ERROR: No se pudo actualizar la tabla en la configuración");
            $sqlServer->Close();
            $mySqlReader->Close();
            $mySqlReader->Close();
            return 1;
        }

        // Valida la tabla de destino.
        $n = $i + 1;
        showMessage("\nValidando tabla $tableName... ($n de $totalSourceTables)");

        // Toma la estructura de la tabla origen.
        showMessage("   Cargando estructura de origen...");
        $sqlCommand =
            "select
                t.name as tablename,
                d.column_id,
                d.name as columnname,
                d.system_type_id,
                d.max_length,
                d.scale
            from
                sys.tables as t
                inner join sys.columns as d on d.object_id = t.object_id
            where
                t.name = '$tableName'";
        $tableStructure = $sqlServer->Query($sqlCommand);

        // Si se quiere eliminar el campo __db_mirror_id__.
        if ($removeIdField) {
            // Valida que exista el campo __db_mirror_id__ en la tabla de origen.
            $found = false;
            for ($j = 0; $j < count($tableStructure); $j++) {
                if ($tableStructure[$j]['columnname'] == '__db_mirror_id__') {
                    $found = true;
                    break;
                }
            }

            // Si existe el campo __db_mirror_id__.
            if ($found) {
                showMessage("   Eliminando campo __db_mirror_id__ de la tabla $tableName...");
                $constraint = '__' . $tableName . '_db_mirror_id_constraint__';
                $sqlCommand = "alter table $tableName drop constraint $constraint;";
                $sqlCommand .= "alter table $tableName drop column __db_mirror_id__;";
                if ($sqlServer->QueryFlat($sqlCommand) === false) {
                    showMessage("ERROR: No se pudo eliminar el campo __db_mirror_id__ de la tabla $tableName");
                    $sqlServer->Close();
                    $mySqlReader->Close();
                    $mySqlReader->Close();
                    return 1;
                }
                showMessage("   Campo __db_mirror_id__ eliminado con exito...");
            }

            // Si solo se estaba procesando una tabla.
            if ($processTableName) {
                break;
            }

            continue;
        }
        
        // Toma la lista de indices de la tabla origen.
        showMessage("   Cargando indices de origen...");
        $sqlCommand = "exec sp_helpindex $tableName";
        $indexList = $sqlServer->Query($sqlCommand);

        if (gettype($indexList) != 'array') {
            $indexList = array();
        }

        // Buscar el campo __db_mirror_id__ en la tabla de origen.
        $found = false;
        for ($j = 0; $j < count($tableStructure); $j++) {
            if ($tableStructure[$j]['columnname'] == '__db_mirror_id__') {
                $found = true;
                break;
            }
        }

        // Si no existe el campo __db_mirror_id__ en la tabla de origen.
        $deleteAllTarget = false;
        if (!$found) {
            // Agrega el campo __db_mirror_id__ a la tabla de origen.
            showMessage("   Creando campo __db_mirror_id__ en el origen...");
            $constraint = '__' . $tableName . '_db_mirror_id_constraint__';
            $sqlCommand = "alter table $tableName add __db_mirror_id__ int not null constraint $constraint default 0;";
            if ($sqlServer->Query($sqlCommand, false) === false) {
                showMessage("ERROR: No se pudo agregar el campo __db_mirror_id__ en la tabla $tableName");
                $sqlServer->Close();
                $mySqlReader->Close();
                $mySqlReader->Close();
                return 1;
            }
            $deleteAllTarget = true;
        }

        // Completa el campo __db_mirror_id__ con una secuencia ascendente de valores
        // enteros para __db_mirror_id__ == 0.
        showMessage("   Inicializando __db_mirror_id__ para valores faltantes...");
        $sqlCommand = "declare @i int;";
        $sqlCommand .= "set @i = (select top 1 t.__db_mirror_id__ from $tableName as t order by __db_mirror_id__ desc);";
        $sqlCommand .= "update $tableName set __db_mirror_id__ = @i, @i = @i + 1 where __db_mirror_id__ = 0;";
        if ($sqlServer->Query($sqlCommand, false) === false) {
            showMessage("ERROR: No se pudo inicializar __db_mirror_id__ en la tabla $tableName");
            $sqlServer->Close();
            $mySqlReader->Close();
            $mySqlReader->Close();
            return 1;
        }

        // Toma la lista de campos validos para la consulta.
        $fieldList = getValidFields($tableStructure);

        // Toma la cantidad total de registros de la tabla origen.
        $sqlCommand = "select count(*) as total from $tableName";
        $cursor = $sqlServer->Query($sqlCommand);
        if ($cursor === false) {
            showMessage("ERROR: No se pudo obtener el total de registros de la tabla $tableName");
            $sqlServer->Close();
            $mySqlReader->Close();
            $mySqlReader->Close();
            return 1;
        }

        // Toma el total de registros a procesar.
        if (count($cursor) > 0) {
            $n = intval($cursor[0]['total']);
        } else {
            $n = 0;
        }

        // Toma la lista de registros de la tabla de origen.
        showMessage("   Procesando registros de origen [$n Registro(s)]...");
        $maxRecordsByQuery = __max_records_by_query;
        $sqlCommand = "select top $maxRecordsByQuery __db_mirror_id__, $fieldList from $tableName where __db_mirror_id__ > 0 order by __db_mirror_id__;";
        $sourceData = $sqlServer->Query($sqlCommand, true, __max_buffer_size);

        if ($sourceData === false) {
            showMessage("ERROR: " . $sqlServer->GetErrorMessage());
            $sqlServer->Close();
            $mySqlReader->Close();
            $mySqlReader->Close();
            return 1;
        }

        $totalSource = count($sourceData);

        // Busca la tabla en la base de datos de destino.
        $found = false;
        for ($j = 0; $j < count($targetTableList); $j++) {
            if ($tableName == $targetTableList[$j]) {
                $found = true;
                break;
            }
        }

        // Si la tabla existe pero se debe crear la tabla.
        if ($found && $createTable) {
            showMessage("   Eliminando tabla $tableName en destino...");
            $sqlCommand = "drop table $tableName";
            if ($mySqlReader->Query($sqlCommand) === false) {
                showMessage("ERROR: " . $sqlServer->GetErrorMessage());
                $sqlServer->Close();
                $mySqlReader->Close();
                $mySqlReader->Close();
                return 1;
            };
            $found = false;
        }

        // Valida la existencia de la tabla en destino.
        if ($found) {
            showMessage("   La tabla $tableName ya existe en destino...");
        } else {
            // Crea la tabla.
            showMessage("   Creando tabla en destino...");
            $sqlCommand = getCreateTableCommand($tableName, $tableStructure);
            if ($sqlCommand === false) {
                showMessage("ERROR: tipo de campo desconocido (revise el registro de eventos)");
                $sqlServer->Close();
                $mySqlReader->Close();
                $mySqlReader->Close();
                return 1;
            }

            if ($mySqlReader->Query($sqlCommand) === false) {
                showMessage("ERROR: No se pudo crear la tabla $tableName");
                $sqlServer->Close();
                $mySqlReader->Close();
                $mySqlReader->Close();
                return 1;
            };

            // Crea los indices.
            for ($j = 0; $j < count($indexList); $j++) {
                $indexName = $indexList[$j]['index_name'];
                $indexKeys =  $indexList[$j]['index_keys'];
                showMessage("   Creando indice '$indexName' => ($indexKeys)");
                $sqlCommand = "create index $indexName on $tableName ($indexKeys)";
                if ($mySqlReader->Query($sqlCommand) === false) {
                    showMessage("ERROR: No se pudo crear el indice $indexName => ($indexKeys)");
                    $sqlServer->Close();
                    $mySqlReader->Close();
                    $mySqlReader->Close();
                    return 1;
                };
            }

            $deleteAllTarget = false;
        }

        // Si debe vaciar la tabla de destino.
        if ($deleteAllTarget) {
            showMessage("   Vaciando tabla de destino...");
            $sqlCommand = "delete from $tableName";
            if ($mySqlWriter->Query($sqlCommand) === false) {
                showMessage("ERROR: No se pudo vaciar la tabla de destino");
                $sqlServer->Close();
                $mySqlReader->Close();
                $mySqlReader->Close();
                return 1;
            }
        }

        // Carga la lista de los registros de destino.
        $sqlCommand = "select t.__db_mirror_id__, t.__md5__ from $tableName as t order by t.__db_mirror_id__";
        $targetData = $mySqlReader->Query($sqlCommand, __max_buffer_size);

        if ($targetData === false) {
            showMessage("ERROR: " . $mySqlReader->GetErrorMessage());
            $sqlServer->Close();
            $mySqlReader->Close();
            $mySqlReader->Close();
            return 1;
        }

        $totalTarget = count($targetData);

        // Inicializa los indices de los buffers origen y destino.
        $si = 0;
        $ti = 0;

        // Inicializa los contadores para la tabla en curso.
        $inserted = 0;
        $updated = 0;
        $deleted = 0;
        $records = 0;

        /**
         * Ciclo de replicacion de la tabla.
         * 
         * Los buffers se encuentran cargados por primera vez, el ciclo debe continuar siempre
         * que cualquiera de los dos buffers sea mayor a cero (0).
         * 
         * En cada incremento de los indices $si o $ti se debe revisar si el buffer quedó vacío
         * y dado el caso se debe hacer un fetch, siempre y cuando el contador no sea cero (0)
         * ya que en este caso es porque ya no quedan registros que procesar en esa tabla.
         */
        while ($totalSource > 0 || $totalTarget > 0) {
            // Si el origen tiene registros.
            if ($si < $totalSource) {
                // Toma el id del registro origen.
                $id = $sourceData[$si]['__db_mirror_id__'];

                // Calcula el hash del registro de origen.
                $hash = md5(json_encode($sourceData[$si]));

                // Si el destino tiene registros.
                if ($ti < $totalTarget) {
                    // Si es el mismo id.
                    if ($id == $targetData[$ti]['__db_mirror_id__']) {
                        // Valida si cambió el md5.
                        if ($hash != $targetData[$ti]['__md5__']) {
                            // Actualiza el registro.
                            $fieldsAndValues = getFieldsAndValues($tableStructure, $sourceData[$si]);
                            $sqlCommand =
                                "update $tableName set
                                    __md5__ = '$hash',
                                    $fieldsAndValues
                                where
                                    __db_mirror_id__ = '$id';";
                            if ($mySqlWriter->Query($sqlCommand) === false) {
                                showMessage('Error: Realizando actualizacion tabla origen => ' . $tableName);
                                $sqlServer->Close();
                                $mySqlReader->Close();
                                $mySqlReader->Close();
                                return 1;
                            }
                            
                            $updated++;
                        }
                        
                        // Contador de registros procesados en esta tabla.
                        $records++;

                        // Incrementa ambos indices.
                        $si++;
                        $ti++;

                        // Valida si debe realizar un fetch.
                        if ($si == $totalSource) {
                            $sourceData = $sqlServer->Fetch(__max_buffer_size);
                            if ($sourceData === false) {
                                showMessage('Error: Realizando fetch en tabla origen => ' . $tableName);
                                $sqlServer->Close();
                                $mySqlReader->Close();
                                $mySqlReader->Close();
                                return 1;
                            }
                            $totalSource = count($sourceData);
                            $si = 0;

                            // Si no quedan registros en el buffer de la ultima consulta.
                            if ($totalSource == 0) {
                                $maxRecordsByQuery = __max_records_by_query;
                                $sqlCommand = "select top $maxRecordsByQuery __db_mirror_id__, $fieldList from $tableName where __db_mirror_id__ > $id order by __db_mirror_id__";
                                $sourceData = $sqlServer->Query($sqlCommand, true, __max_buffer_size);

                                if ($sourceData === false) {
                                    showMessage("ERROR: " . $sqlServer->GetErrorMessage());
                                    $sqlServer->Close();
                                    $mySqlReader->Close();
                                    $mySqlReader->Close();
                                    return 1;
                                }

                                $totalSource = count($sourceData);
                            }
                        }

                        if ($ti == $totalTarget) {
                            $targetData = $mySqlReader->Fetch(__max_buffer_size);
                            if ($targetData === false) {
                                showMessage('Error: Realizando fetch en tabla destino => ' . $tableName);
                                $sqlServer->Close();
                                $mySqlReader->Close();
                                $mySqlReader->Close();
                                return 1;
                            }
                            $totalTarget = count($targetData);
                            $ti = 0;
                        }
                    }

                    // No tienen el mmismo id.
                    else {
                        // Elimina del destino el registro con el id.
                        $sqlCommand = "delete from $tableName where __db_mirror_id__ = '$id';";

                        if ($mySqlWriter->Query($sqlCommand) === false) {
                            showMessage('Error: Realizando eliminando registro en tabla destino => ' . $tableName);
                            $sqlServer->Close();
                            $mySqlReader->Close();
                            $mySqlReader->Close();
                            return 1;
                        }
                        
                        $deleted++;
                        $records++;

                        // Incrementa solo el indice de destino.
                        $ti++;

                        // Valida si debe realizar un fetch.
                        if ($ti == $totalTarget) {
                            $targetData = $mySqlReader->Fetch(__max_buffer_size);
                            if ($targetData === false) {
                                showMessage('Error: Realizando fetch en tabla destino => ' . $tableName);
                                $sqlServer->Close();
                                $mySqlReader->Close();
                                $mySqlReader->Close();
                                return 1;
                            }
                            $totalTarget = count($targetData);
                            $ti = 0;
                        }
                    }
                }

                // Si el destino no tiene registros por procesar.
                else {
                    // Agrega el registro a la cola de sentencias.
                    $valueList = getValidValues($tableStructure, $sourceData[$si]);
                    $sqlInsert = "('$id', '$hash', $valueList)";
                    $sqlCommand = "insert into $tableName (__db_mirror_id__, __md5__, $fieldList) values " . $sqlInsert . ';';
                    
                    if ($mySqlWriter->Query($sqlCommand) === false) {
                        showMessage('Error: Realizando la insercion en tabla destino => ' . $tableName);
                        $sqlServer->Close();
                        $mySqlReader->Close();
                        $mySqlReader->Close();
                        return 1;
                    }

                    $inserted++;
                    $records++;

                    // Incrementa solamente el indice de origen.
                    $si++;

                    // Valida si debe realizar un fetch.
                    if ($si == $totalSource) {
                        $sourceData = $sqlServer->Fetch(__max_buffer_size);
                        if ($sourceData === false) {
                            showMessage('Error: Realizando fetch en tabla origen => ' . $tableName);
                            $sqlServer->Close();
                            $mySqlReader->Close();
                            $mySqlReader->Close();
                            return 1;
                        }
                        $totalSource = count($sourceData);
                        $si = 0;

                        // Si no quedan registros en el buffer de la ultima consulta.
                        if ($totalSource == 0) {
                            $maxRecordsByQuery = __max_records_by_query;
                            $sqlCommand = "select top $maxRecordsByQuery __db_mirror_id__, $fieldList from $tableName where __db_mirror_id__ > $id order by __db_mirror_id__";
                            $sourceData = $sqlServer->Query($sqlCommand, true, __max_buffer_size);

                            if ($sourceData === false) {
                                showMessage("ERROR: " . $sqlServer->GetErrorMessage());
                                $sqlServer->Close();
                                $mySqlReader->Close();
                                $mySqlReader->Close();
                                return 1;
                            }

                            $totalSource = count($sourceData);
                        }
                    }
                }
            }

            // Si el origen ya no tiene registros.
            else {
                // Si quedan registros en el destino.
                if ($ti < $totalTarget) {
                    // Elimina del destino el registro con el id.
                    $sqlCommand = "delete from $tableName where __db_mirror_id__ = '$id';";
                    
                    if ($mySqlWriter->Query($sqlCommand) === false) {
                        showMessage('Error: Eliminando registro en tabla destino => ' . $tableName);
                        $sqlServer->Close();
                        $mySqlReader->Close();
                        $mySqlReader->Close();
                        return 1;
                    }

                    $deleted++;
                    $records++;

                    // Incrementa solo el indice de destino.
                    $ti++;

                    // Valida si debe realizar un fetch.
                    if ($ti == $totalTarget) {
                        $targetData = $mySqlReader->Fetch(__max_buffer_size);
                        if ($targetData === false) {
                            showMessage('Error: Realizando fetch en tabla destino => ' . $tableName);
                            $sqlServer->Close();
                            $mySqlReader->Close();
                            $mySqlReader->Close();
                            return 1;
                        }
                        $totalTarget = count($targetData);
                        $ti = 0;
                    }
                }
            }

            $totalInserted += $inserted;
            $totalUpdated += $updated;
            $totalDeleted += $deleted;
            $totalRecords += $records;

            // Muestra el progreso de la replicacion.
            $p = intval(($records * 100) / $n);
            showMessage("   Progreso => $records de $n registros | ($p%) | Ins: $inserted / Act: $updated / Elim: $deleted", "\r");
        }

        // Si la tabla estaba vacia.
        if ($records == 0) {
            showMessage('   Tabla vacía...', "\r");
        }

        // Deja una linea de separacion.
        showMessage('');

        // Si solo debe procesar una tabla especifica.
        if ($processTableFlag) {
            break;
        }

        // Pasa a la siguiente tabla.
        $i++;
    }

    // Restablece el registro de configuracion.
    $lastUpdate = date("Y-m-d h:i:s");
    $sqlCommand =
        "update $configTableName set
            on_process = '',
            lastupdate = '$lastUpdate'
        where id = '1'";
    if ($mySqlWriter->Query($sqlCommand) === false) {
        showMessage("ERROR: " . $mySqlWriter->GetErrorMessage());
        $sqlServer->Close();
        $mySqlReader->Close();
        $mySqlReader->Close();
        return 1;
    }

    // Finaliza las conexiones con las bases de datos.
    $sqlServer->Close();
    $mySqlReader->Close();
    $mySqlReader->Close();

    // Muestra el resumen del proceso.
    showMessage('');
    showMessage("Total registros insertados: $totalInserted");
    showMessage("Total registros actualizados: $totalUpdated");
    showMessage("Total registros eliminados: $totalDeleted");
    showMessage("Total registros procesados: $totalRecords");
    
    // Muestra la hora de inicio y fin.
    $endTime = date("d-m-Y h:i:s");
    showMessage('');
    showMessage("Inicio: $startTime");
    showMessage("Fin: $endTime");

    return 0;
?>
