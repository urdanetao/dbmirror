<?php
    /**
     * Devuelve el arreglo DbInfo necesario para conectarse a SQL Server.
     */
    function getSqlServerDbInfo() {
        $dbInfo = array(
            'host' => '192.168.2.1',
            'prefix' => '',
            'dbname' => 'saiverdb',
            'user' => 'sa',
            'pwd' => '-SqlServer**'
        );
        return $dbInfo;
    }


    /**
     * Devuelve el arreglo DbInfo necesario para conectarse a MySql.
     */
    function getMySqlDbInfo() {
        $dbInfo = array(
            'host' => 'server21.websiteplex.com',
            'prefix' => 'almacena_',
            'dbname' => 'saiverdb',
            'user' => 'dbuser',
            'pwd' => 'admin'
        );
        return $dbInfo;
    }
?>
