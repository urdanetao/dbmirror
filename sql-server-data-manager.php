<?php
    /**
     * Clase SqlServerDataManager
     * 
     * Clase para manejar bases de datos SQL Server.
     * Dev. Oscar E. Urdaneta
     * Fecha: 2024-01-29
     */
    class SqlServerDataManager {
        /**
         * Definicion de constantes.
         */

        // Nombre del archivo de registro de eventos.
        private const __SQL_SERVER_DATA_MANAGER_LOG_FILENAME = 'sql-server-data-manager.log';

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

            $connectionOptions = array(
                "database" => $dbname,
                "uid" => $user,
                "pwd" => $pwd
            );

            $conn = sqlsrv_connect($host, $connectionOptions);

            // Si falló la conexión.
            if ($conn === false) {
                $this->ErrorHandler(sqlsrv_errors());
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
         * Manejador de errores.
         * 
         * @param e - (string ó array) Cadena o array devuelto por objeto sqlsrv_errors().
         * 
         * Obtiene el ultimo error de sql server y de acuerdo a la configuracion guarda el log.
         */
        private function ErrorHandler($e) {
            if ($this->saveEventLog) {
                $type = gettype($e);
                if ($type == 'array') {
                    if (count($e) > 0 && isset($e[0]['code']) && isset($e[0]['message'])) {
                        $this->errorMessage = $e[0]['code'] . ' - ' . $e[0]['message'];
                    } else {
                        $this->errorMessage = 'ErrorHandler: el arreglo $e no tiene registros o la estructura necesaria' . PHP_EOL .
                        var_export($e, true);
                    }
                } elseif ($type == 'string') {
                    $this->errorMessage = $e;
                } else {
                    $this->errorMessage = 'ErrorHandler: Parametro $e de tipo no soportado -> ' . $type;
                }
                $this->SaveEventLog($this->errorMessage);
            }
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
         * @param sqlCommand - (string) Sentencia sql a ejecutar.
         * @param hasData - (boolean) Indica si la consulta devuelve resultados como registros.
         * @param maxRows - (integer) Indica el numero maximo de registros a extraer del buffer.
         * 
         * Ejecuta una consulta sql, la consulta puede devolver registros como resultado.
         * 
         * Si falla retorna falso y establece el mensaje de error.
         */
        public function Query($sqlCommand, $hasData = true, $maxRows = 0) {
            // Valida los parametros.
            if (gettype($sqlCommand) != 'string') {
                $this->ErrorHandler('Query: El parametro [sqlCommand] debe ser de tipo string');
                return false;
            }

            if (gettype($hasData) != 'boolean') {
                $this->ErrorHandler('Query: El parametro [hasData] debe ser de tipo boolean');
                return false;
            }

            if (gettype($maxRows) != 'integer') {
                $this->ErrorHandler('Query: El parametro [maxRows] debe ser de tipo numerico');
                return false;
            }

            if ($maxRows < 0) {
                $this->ErrorHandler('Query: El parametro [maxRows] debe ser un numero positivo');
                return false;
            }

            // Valida que exista una conexion activa.
            if (!$this->connected) {
                $this->ErrorHandler('Query: No hay una conexión activa');
                return false;
            }

            // Valida que no hayan resultados pendientes por extraer.
            if ($this->stmt !== false) {
                $this->ErrorHandler('Query: Hay resultados pendientes por extraer');
                return false;
            }

            // Ejecuta la sentencia.
            saveLog($sqlCommand);
            $stmt = sqlsrv_query($this->conn, $sqlCommand);

            // Si falla la ejecusion.
            if ($stmt === false) {
                $this->ErrorHandler(sqlsrv_errors());
                return false;
            }

            // Si la consulta no devuelve resultados.
            if (!$hasData) {
                return true;
            }

            // Extrae los registros en un arreglo asociativo.
            $this->stmt = $stmt;
            $data = $this->Fetch($maxRows);
            if ($data === false) {
                $this->ErrorHandler('Query: Falló la llamada a fecth');
                return false;
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
            // Inicializa el array con los resultados.
            $data = array();

            // Valida los parametros.
            if (gettype($maxRows) != 'integer') {
                $this->ErrorHandler('Fetch: El parametro [maxRows] debe ser de tipo numerico');
                return false;
            }

            if ($maxRows < 0) {
                $this->ErrorHandler('Fetch: El parametro [maxRows] debe ser un numero positivo');
                return false;
            }

            // Valida que exista una conexion activa.
            if (!$this->connected) {
                $this->ErrorHandler('Fetch: No hay una conexión activa');
                return false;
            }

            // Si no hay resultados pendientes.
            if ($this->stmt === false) {
                return $data;
            }

            $this->errorMessage = '';

            // Extrae los registros en un arreglo asociativo.
            $i = 0;
            
            // Valida que existan resultados pendientes por extraer.
            while (true) {
                // Toma el registro.
                $row = sqlsrv_fetch_array($this->stmt, SQLSRV_FETCH_ASSOC);

                // Si ocurrio un error.
                if ($row === false) {
                    saveLog('El fetch se tiro 3 peos');
                    $this->ErrorHandler(sqlsrv_errors());
                    return false;
                }

                // No hay mas registros.
                if ($row === null ){
                    $this->stmt = false;
                    break;
                }

                // Guarda el registro en el arreglo.
                $data[$i] = $row;
                $i++;

                // Si alcanzó el maximo de registros solicitados.
                if ($maxRows > 0 && $i == $maxRows) {
                    break;
                }
            }
            
            // Devuelve los resultados.
            return $data;
        }


        /**
         * Metodo GetDatabases.
         * 
         * Devuelve un arreglo con la lista de bases de datos.
         */
        public function GetDatabases() {
            $sqlCommand = "select t.name from sys.databases as t;";
            $result = $this->Query($sqlCommand);

            if ($result === false) {
                return $result;
            }

            // Convierte a minusculas los nombres de las bases de datos.
            $data = array();
            for ($i = 0; $i < count($result); $i++) {
                $data[$i] = strtolower($result[$i]['name']);
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
         * 
         * NOTA: El nombre de la base de datos buscada debe estar en minusculas.
         */
        public function ExistDatabase($name) {
            // Valida los parametros.
            if (gettype($name) != 'string') {
                $this->ErrorHandler('ExistDatabase: El tipo de [name] debe ser string');
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
            $sqlCommand =
                "select
                    t.table_name as tablename
                from
                    information_schema.tables as t
                where
                    t.table_schema = 'dbo' and
                    t.table_type = 'BASE TABLE'
                order by
                    t.table_name;";
            $result = $this->Query($sqlCommand);

            if ($result === false) {
                return $result;
            }

            // Convierte a minusculas los nombres de las tablas.
            $data = array();
            for ($i = 0; $i < count($result); $i++) {
                $data[$i] = strtolower($result[$i]['tablename']);
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
         * 
         * NOTA: El nombre de la tabla a buscar debe estar en minusculas.
         */
        public function ExistTable($name) {
            // Valida los parametros.
            if (gettype($name) != 'string') {
                $this->ErrorHandler('ExistTable: El tipo de [name] debe ser string');
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
                sqlsrv_close($this->conn);
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
                $this->ErrorHandler('SetEventLogState: El parametro [state] debe ser de tipo booleano');
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
                $this->ErrorHandler('El parametro [dbInfo] debe ser un arreglo asociativo');
                return false;
            }

            if (!isset($dbInfo['host'])) {
                $this->ErrorHandler('No se estableció el campo [host] en el parametro [dbInfo]');
                return false;
            }

            if (gettype($dbInfo['host']) != 'string') {
                $this->ErrorHandler('El campo [host] debe ser de tipo cadena');
                return false;
            }

            if (!isset($dbInfo['prefix'])) {
                $this->ErrorHandler('No se estableció el campo [prefix] en el parametro [dbInfo]');
                return false;
            }

            if (gettype($dbInfo['prefix']) != 'string') {
                $this->ErrorHandler('El campo [prefix] debe ser de tipo cadena');
                return false;
            }

            if (!isset($dbInfo['dbname'])) {
                $this->ErrorHandler('No se estableció el campo [dbname] en el parametro [dbInfo]');
                return false;
            }

            if (gettype($dbInfo['dbname']) != 'string') {
                $this->ErrorHandler('El campo [dbname] debe ser de tipo cadena');
                return false;
            }

            if (!isset($dbInfo['user'])) {
                $this->ErrorHandler('No se estableció el campo [user] en el parametro [dbInfo]');
                return false;
            }

            if (gettype($dbInfo['user']) != 'string') {
                $this->ErrorHandler('El campo [user] debe ser de tipo cadena');
                return false;
            }

            if (!isset($dbInfo['pwd'])) {
                $this->ErrorHandler('No se estableció el campo [pwd] en el parametro [dbInfo]');
                return false;
            }

            if (gettype($dbInfo['pwd']) != 'string') {
                $this->ErrorHandler('El campo [pwd] debe ser de tipo cadena');
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
                $file = fopen(__DIR__ . '/' . SqlServerDataManager::__SQL_SERVER_DATA_MANAGER_LOG_FILENAME, "a");
                $data =
                    PHP_EOL .
                    date('Y-m-d H:i:s') . PHP_EOL .
                    $log . PHP_EOL;
                fwrite($file, $data . ' ' . $log . PHP_EOL);
                fclose($file);
            }
        }
    }
?>
