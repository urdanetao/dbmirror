<?php
    /**
     * Clase MySqlDataManager
     * 
     * Clase para manejar bases de datos MySql.
     * Dev. Oscar E. Urdaneta
     * Fecha: 2023-05-06
     */
    class MySqlDataManager {
        /**
         * Definicion de constantes.
         */
        private const __MYSQL_DATA_MANAGER_LOG_FILENAME = 'mysql-data-manager-log';

        /**
         * Propiedades privadas.
         */
        private $connected;
        private $conn;
        private $stmt;
        private $errorMessage;
        private $saveEventLog;

        /**
         * Constructor.
         * @param dbInfo - Arreglo asociativo con la informacion de conexion a la base de datos.
         * 
         * Campos de dbInfo.
         * host: Nombre del host o direccion IP.
         * prefix: Prefijo de la base de datos, se puede omitir el valor.
         * dbname: Nombre de la base de datos, se puede omitir el valor.
         * user: Nombre del usuario de la base de datos.
         * pwd: Contraseña.
         * 
         * Crea una conexion dentro de la clase, para determinar si se realizo la conexión se
         * debe invocar el metodo IsConnected().
         */
        function __construct($dbInfo) {
            // Establece los valores por defecto.
            $this->connected = false;
            $this->conn = false;
            $this->stmt = false;
            $this->errorMessage = '';
            $this->saveEventLog = true;

            // Valida la estructura del arreglo asociativo dbInfo.
            if (!$this->ValidateDbInfoStructure($dbInfo)) {
                return;
            }

            // Intenta conectar con la base de datos.
            $host = $dbInfo['host'];
            if ($dbInfo['dbname'] != '') {
                $dbname = $dbInfo['prefix'] . $dbInfo['dbname'];
                $user = $dbInfo['prefix'] . $dbInfo['user'];
            } else {
                $dbname = '';
                $user = $dbInfo['user'];
            }
            $pwd = $dbInfo['pwd'];
            $conn = mysqli_connect($host, $user, $pwd, $dbname);

            // Si fallo la conexion.
            if ($conn === false) {
                $this->errorMessage = mysqli_connect_error();
                $this->SaveEventLog($this->errorMessage);
                return;
            }

            $this->errorMessage = '';
            $this->conn = $conn;
            $this->connected = true;
        }

        /**
         * Destructor.
         * 
         * Garantiza que se cierre la conexion con la base de datos.
         */
        function __destruct() {
            $this->Close();
        }

        /**
         * Metodo IsConnected
         * 
         * Retorna true si hay una conexión establecida con la base de datos, de lo contrario
         * retorna false.
         */
        public function IsConnected() {
            return $this->connected;
        }

        /**
         * Metodo Query
         * 
         * @param sqlCommand - Sentencia sql a ejecutar.
         * @param maxRows - Numero maximo de registros a extraer del buffer, todos por defecto.
         * 
         * Ejecuta una consulta sql simple, la consulta puede devolver resultados.
         * 
         * Si falla retorna falso y establece el mensaje de error.
         */
        public function Query($sqlCommand, $maxRows = 0) {
            // Valida los parametros.
            if (gettype($sqlCommand) != 'string') {
                $this->errorMessage = 'Query: El parametro [sqlCommand] debe ser de tipo string';
                $this->SaveEventLog($this->errorMessage);
                return false;
            }

            if (gettype($maxRows) != 'integer') {
                $this->errorMessage = 'Query: El parametro [maxRows] debe ser de tipo numerico';
                $this->SaveEventLog($this->errorMessage);
                return false;
            }

            // Valida que exista una conexion activa.
            if (!$this->connected) {
                $this->errorMessage = 'Query: No hay una conexión activa';
                $this->SaveEventLog($this->errorMessage);
                return false;
            }

            // Valida que no hayan resultados pendientes por extraer.
            if ($this->stmt !== false) {
                $this->errorMessage = 'Query: Hay resultados pendientes por extraer';
                $this->SaveEventLog($this->errorMessage);
                return false;
            }

            // Establece el autocommit.
            if ($this->conn->autocommit(true) === false) {
                $this->errorMessage = 'Query: No se pudo establecer el autocommit';
                $this->SaveEventLog($this->errorMessage);
                return false;
            }

            // Ejecuta la sentencia.
            $stmt = $this->conn->query($sqlCommand);

            if (gettype($stmt) == 'boolean') {
                // Si falla la ejecusion.
                if ($stmt === false) {
                    $this->errorMessage = 'Query: ' . $this->conn->error();
                    $this->SaveEventLog($this->errorMessage);
                    return false;
                }

                $this->errorMessage = '';
                return true;
            }

            // Extrae los registros en un arreglo asociativo.
            $i = 0;
            $data = array();
            while (true) {
                // Toma el registro.
                $row = $stmt->fetch_assoc();

                // Si falló.
                if ($row === false) {
                    $this->errorMessage = 'Query: ' . $this->conn->error();
                    $this->SaveEventLog($this->errorMessage);
                    $this->SaveEventLog($sqlCommand);
                    return false;
                }

                // Si no hay mas registros por extraer.
                if (is_null($row)) {
                    $stmt->close();
                    $this->stmt = false;
                    break;
                }

                // Guarda el registro en el arreglo.
                $data[$i] = $row;
                $i++;

                // Si alcanzó el maximo de registros solicitados.
                if ($maxRows != 0 && $i == $maxRows) {
                    $this->stmt = $stmt;
                    break;
                }
            }

            $this->errorMessage = '';

            // Devuelve los resultados.
            return $data;
        }

        /**
         * Metodo Fetch
         * 
         * @param maxRows - Numero maximo de registros a recuperar.
         * 
         * Carga registros desde el buffer de una consulta previa.
         * 
         * Retorna un arreglo asociativo con los registros o false si falla.
         */
        public function Fetch($maxRows = 0) {
            // Valida los parametros.
            if (gettype($maxRows) != 'integer') {
                $this->errorMessage = 'Query: El parametro [maxRows] debe ser de tipo numerico';
                $this->SaveEventLog($this->errorMessage);
                return false;
            }

            // Valida que exista una conexion activa.
            if (!$this->connected) {
                $this->errorMessage = 'Query: No hay una conexión activa';
                $this->SaveEventLog($this->errorMessage);
                return false;
            }

            // Extrae los registros en un arreglo asociativo.
            $i = 0;
            $data = array();

            // Valida que existan resultados pendientes por extraer.
            if ($this->stmt === false) {
                return $data;
            }

            while (true) {
                // Toma el registro.
                $row = $this->stmt->fetch_assoc();

                // Si falló.
                if ($row === false) {
                    $this->errorMessage = 'Fetch: ' . $this->conn->error();
                    $this->SaveEventLog($this->errorMessage);
                    return false;
                }

                // Si no hay mas registros por extraer.
                if (is_null($row)) {
                    $this->stmt->close();
                    $this->stmt = false;
                    break;
                }

                // Guarda el registro en el arreglo.
                $data[$i] = $row;
                $i++;

                // Si alcanzó el maximo de registros solicitados.
                if ($maxRows != 0 && $i == $maxRows) {
                    break;
                }
            }

            $this->errorMessage = '';

            // Devuelve los resultados.
            return $data;
        }

        /**
         * Metodo MultiQuery
         * 
         * @param sqlCommand - Lista de sentencias sql a ejecutar.
         * 
         * Ejecuta multiples sentencias sql contenidas en una cadena de caracteres
         * separadas por un punto y coma (;).
         * 
         * Todos los resultados de las sentencias son desechados.
         * 
         * Devuelve true si todas las sentencias son ejecutadas con exito o false
         * si cualquiera de las sentencias falla.
         */
        public function MultiQuery($sqlCommand) {
            // Valida los parametros.
            if (gettype($sqlCommand) != 'string') {
                $this->errorMessage = 'MultiQuery: El parametro [sqlCommand] debe ser de tipo string';
                $this->SaveEventLog($this->errorMessage);
                return false;
            }

            // Valida que exista una conexion activa.
            if (!$this->connected) {
                $this->errorMessage = 'MultiQuery: No hay una conexión activa';
                $this->SaveEventLog($this->errorMessage);
                return false;
            }

            // Valida que no hayan resultados pendientes por extraer.
            if ($this->stmt !== false) {
                $this->errorMessage = 'MultiQuery: Hay resultados pendientes por extraer';
                $this->SaveEventLog($this->errorMessage);
                return false;
            }

            // Establece el autocommit.
            if ($this->conn->autocommit(true) === false) {
                $this->errorMessage = 'MultiQuery: No se pudo establecer el autocommit';
                $this->SaveEventLog($this->errorMessage);
                return false;
            }

            // Ejecuta la sentencia.
            if ($this->conn->multi_query($sqlCommand) === false) {
                $this->errorMessage = 'MultiQuery: la consulta falló (1) . PHP_EOL . $sqlCommand';
                $this->SaveEventLog($this->errorMessage);
                return false;
            }

            do {
                $stmt = $this->conn->store_result();
                if ($stmt === false && $this->conn->errno != 0) {
                    $this->errorMessage = 'MultiQuery: la consulta falló (2)' . PHP_EOL . $sqlCommand;
                    $this->SaveEventLog($this->errorMessage);
                    return false;
                }
                if (gettype($stmt) != 'boolean') {
                    $stmt->free();
                }
            } while ($this->conn->next_result());

            $this->errorMessage = '';

            // Devuelve los resultados.
            return true;
        }

        /**
         * Metodo GetDatabases.
         * 
         * Devuelve un arreglo con la lista de bases de datos.
         */
        public function GetDatabases() {
            $sqlCommand = "show databases;";
            $result = $this->Query($sqlCommand);

            if ($result === false) {
                return $result;
            }

            $i = 0;
            $data = array();
            for ($j = 0; $j < count($result); $j++) {
                foreach ($result[$j] as $key => $value) {
                    $data[$i] = strtolower($value);
                    $i++;
                    break;
                }
            }

            $this->errorMessage = '';
            return $data;
        }

        /**
         * Metodo ExistDatabase.
         * 
         * @param name - Nombre de la base de datos que se desea buscar.
         * 
         * Devuelve true si existe la base de datos indicada en la base de datos, de lo
         * contrario retorna false.
         */
        public function ExistDatabase($name) {
            // Valida los parametros.
            if (gettype($name) != 'string') {
                $this->errorMessage = 'ExistDatabase: El tipo de [name] debe ser string';
                $this->SaveEventLog($this->errorMessage);
                return false;
            }

            // Toma la lista de tablas registradas.
            $result = $this->GetDatabases();

            if ($result === false) {
                return $result;
            }

            $found = false;
            for ($i = 0; $i < count($result); $i++) {
                if ($result[$i] == $name) {
                    $found = true;
                    break;
                }
            }

            $this->errorMessage = '';
            return $found;
        }

        /**
         * Metodo GetTables.
         * 
         * Devuelve un arreglo con las tablas que contiene la base de datos.
         */
        public function GetTables() {
            $sqlCommand = "show tables;";
            $result = $this->Query($sqlCommand);

            if ($result === false) {
                return $result;
            }

            $i = 0;
            $data = array();
            for ($j = 0; $j < count($result); $j++) {
                foreach ($result[$j] as $key => $value) {
                    $data[$i] = strtolower($value);
                    $i++;
                    break;
                }
            }

            $this->errorMessage = '';
            return $data;
        }

        /**
         * Metodo ExistTable.
         * 
         * @param name - Nombre de la base de tabla que se desea buscar.
         * 
         * Devuelve true si existe la tabla indicada existe en la base de datos, de lo
         * contrario retorna false.
         */
        public function ExistTable($name) {
            // Valida los parametros.
            if (gettype($name) != 'string') {
                $this->errorMessage = 'ExistTable: El tipo de [name] debe ser string';
                $this->SaveEventLog($this->errorMessage);
                return false;
            }

            // Toma la lista de tablas registradas.
            $result = $this->GetTables();

            if ($result === false) {
                return $result;
            }

            $found = false;
            for ($i = 0; $i < count($result); $i++) {
                if ($result[$i] == $name) {
                    $found = true;
                    break;
                }
            }

            $this->errorMessage = '';
            return $found;
        }

        /**
         * Metodo Close.
         * 
         * Termina la conexion con la base de datos.
         */
        public function Close() {
            if ($this->connected) {
                // Si hay resultados pendientes.
                if ($this->stmt !== false) {
                    // Extrae todos los resultados pendientes.
                    $this->Fetch();
                }

                // Termina la conexion.
                $this->conn->close();
                $this->conn = false;
                $this->connected = false;
            }
        }

        /**
         * Metodo GetErrorMessage.
         * 
         * Devuelve el ultimo mensaje de error registrado.
         */
        public function GetErrorMessage() {
            return $this->errorMessage;
        }

        /**
         * Metodo SetEventLogState
         * 
         * Establece si se debe guardar el registro de eventos en el archivo, por
         * defecto es true
         */
        public function SetEventLogState($state) {
            if (gettype($state) != 'boolean') {
                $this->errorMessage = 'SetEventLogState: El parametro [state] debe ser de tipo booleano';
                $this->SaveEventLog($this->errorMessage);
                return;
            }
            $this->saveEventLog = $state;
        }
        
        /**
         * DECLARACION DE METODOS PRIVADOS.
         */
    
        /**
         * Metodo ValidateDbInfoStructure
         * 
         * Valida la estructura del arreglo asociativo dbInfo.
         * 
         * Retorna true si todo esta correcto o false de lo contrario, si algo no esta
         * correcto se puede obtener el mensaje de error con el metodo GetErrorMessage()
         */
        private function ValidateDbInfoStructure($dbInfo) {
            if (gettype($dbInfo) != 'array') {
                $this->errorMessage = 'El parametro [dbInfo] debe ser un arreglo asociativo';
                $this->SaveEventLog($this->errorMessage);
                return false;
            }

            if (!isset($dbInfo['host'])) {
                $this->errorMessage = 'No se estableció el campo [host] en el parametro [dbInfo]';
                $this->SaveEventLog($this->errorMessage);
                return false;
            }

            if (gettype($dbInfo['host']) != 'string') {
                $this->errorMessage = 'El campo [host] debe ser de tipo cadena';
                $this->SaveEventLog($this->errorMessage);
                return false;
            }

            if (!isset($dbInfo['prefix'])) {
                $this->errorMessage = 'No se estableció el campo [prefix] en el parametro [dbInfo]';
                $this->SaveEventLog($this->errorMessage);
                return false;
            }

            if (gettype($dbInfo['prefix']) != 'string') {
                $this->errorMessage = 'El campo [prefix] debe ser de tipo cadena';
                $this->SaveEventLog($this->errorMessage);
                return false;
            }

            if (!isset($dbInfo['dbname'])) {
                $this->errorMessage = 'No se estableció el campo [dbname] en el parametro [dbInfo]';
                $this->SaveEventLog($this->errorMessage);
                return false;
            }

            if (gettype($dbInfo['dbname']) != 'string') {
                $this->errorMessage = 'El campo [dbname] debe ser de tipo cadena';
                $this->SaveEventLog($this->errorMessage);
                return false;
            }

            if (!isset($dbInfo['user'])) {
                $this->errorMessage = 'No se estableció el campo [user] en el parametro [dbInfo]';
                $this->SaveEventLog($this->errorMessage);
                return false;
            }

            if (gettype($dbInfo['user']) != 'string') {
                $this->errorMessage = 'El campo [user] debe ser de tipo cadena';
                $this->SaveEventLog($this->errorMessage);
                return false;
            }

            if (!isset($dbInfo['pwd'])) {
                $this->errorMessage = 'No se estableció el campo [pwd] en el parametro [dbInfo]';
                $this->SaveEventLog($this->errorMessage);
                return false;
            }

            if (gettype($dbInfo['pwd']) != 'string') {
                $this->errorMessage = 'El campo [pwd] debe ser de tipo cadena';
                $this->SaveEventLog($this->errorMessage);
                return false;
            }

            $this->errorMessage = '';
            return true;
        }

        /**
         * Metodo SaveEventLog
         * 
         * Guarda el registro de eventos.
         */
        private function SaveEventLog($log) {
            if ($this->saveEventLog) {
                $file = fopen(__DIR__ . '/' . MySqlDataManager::__MYSQL_DATA_MANAGER_LOG_FILENAME, "a");
                $date = date('Y-m-d H:i:s');
                fwrite($file, $date . ' ' . $log . PHP_EOL);
                fclose($file);
            }
        }
    }
?>
