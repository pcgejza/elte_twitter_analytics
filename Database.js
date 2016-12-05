var pg = require('pg');

DB = {
    host: 'localhost',
    user: 'user_eltereclab',
    pass: 'pass_eltereclab',
    database: 'elte_reclab',
    
    tableName : 'music_',
    
    getConnStr: function () {
        return "postgres://"+this.user+":"+this.pass+"@"+this.host+"/"+this.database;
    },
    
    insert: function (q) {
        pg.connect(this.getConnStr(), function (err, client, done) {
            if (err) {
                return console.error('error fetching client from pool', err);
            }
            client.query(q, null, function (err, result) {
                //call `done()` to release the client back to the pool
                done();

                if (err) {
                    console.error(q);
                    return console.error('error running query', err);
                }
                //console.log('Query executed : '+q);
                //output: 1
            });
        });
    },
    
    
    createTableIfNotExists : function(afterf){
        var ifexistsq = "SELECT EXISTS ("+
                        "SELECT 1 "+
                        "FROM   information_schema.tables "+
                        "WHERE  table_name = '"+this.tableName+"' "+
                        ");"
        var createTableSql = "CREATE TABLE "+this.tableName+" (id SERIAL, music_artist VARCHAR(255), music_name VARCHAR(255));"
        pg.connect(this.getConnStr(), function (err, client, done) {
            if (err) {
                return console.error('error fetching client from pool', err);
            }
            client.query(ifexistsq, null, function (err, result) {
                //call `done()` to release the client back to the pool
                done();

                if (err) {
                    return console.error('error running query', err);
                }
                
                if(!result.rows[0].exists){
                    client.query(createTableSql, null, function (err2, result2) {
                        if (err2) {
                            return console.error('error running query', err2);
                        }
                        afterf();
                    });
                }
            });
        });
    },
    
    
    emptyTable : function(){
        var createTableSql = "DELETE FROM "+this.tableName+";"
        pg.connect(DB.getConnStr(), function (err, client, done) {
            if (err) {
                return console.error('error fetching client from pool', err);
            }
            client.query(createTableSql, null, function (err, result) {
                //call `done()` to release the client back to the pool
                done();

                if (err) {
                    return console.error('error running query', err);
                }
                
                console.log('Table "'+this.tableName+'" emptied')
                
            });
        });
    },
};

