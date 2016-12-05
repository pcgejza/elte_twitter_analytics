var fs = require('fs');
var vm = require('vm')
var csv = require('fast-csv');
var sys = require('sys')
var exec = require('child_process').exec;
var child;
var path = require("path");
var mkdirp = require('mkdirp');
var CONSOLE_LOG = true;
var db = require('./Database.js');

LoadCsv = {
    
    
    REQ_DATAS : [],
    currentPercentage : 0,
    allFilesIt  : 0,
    allFilesLength : 0,
    counter : 0,
    
    
    filepath : './music.tsv',
    tempFolder : './csv/',
    csvDelimiter : '|',
    rowsPerFile : 10000, // milyen sorközönként vágja szét a nagyobb fájlt
    
    SB_TIMES : {
        'SB2014' : {
            'start' : new Date('2013-11-01'), 
            'end' : new Date('2014-02-01'), 
        },
        'SB2015' : {
            'start' :new Date('2014-10-31'), 
            'end' : new Date('2015-01-31'), 
        },
        'SB2016' : {
            'start' : new Date('2015-11-06'), 
            'end' : new Date('2016-02-06'), 
        }
    },
    
    HASHTAGS : {
       'SB2014' : { // Győztes : SEAHAWKS
            BRONCOS :  [
                '#BroncosWin',
                '#GoBroncos',
                '#Broncos',
                '#Denver',
                '#DENVER',
            ],

            SEAHAWKS :  [
                '#SEAHAWKSWin',
                '#seahawks',
                '#GoSeahawks',
                '#Goseahawks',
                '#goseahawks',
                '#Seahawks',
                '#SEAHAWKS',
                '#Seattle'
            ], 
       },
       'SB2015' : { // Győztes: PATRIOTS
            PATRIOTS :  [
                '#Pats',
                '#PATS',
                '#PatsWin',
                '#PatriotsWin',
                '#GoPatriots',
                '#Patriots',
                '#PATRIOTS',
                '#NewEngland',
                '#NEP'
            ],

            SEAHAWKS :  [
                '#SEAHAWKSWin',
                '#seahawks',
                '#GoSeahawks',
                '#Goseahawks',
                '#goseahawks',
                '#Seahawks',
                '#SEAHAWKS',
                '#Seattle'
            ], 
       },
       'SB2016' : { // Győztes : BRONCOS
            BRONCOS :  [
                '#BroncosWin',
                '#GoBroncos',
                '#Broncos',
                '#Denver',
                '#DENVER',
            ],

            PANTHERS :  [
                '#PanthersWin',
                '#pantherswin',
                '#panthers',
                '#Panthers',
                '#Carolina'
            ], 
       },
    },
    
    DAYS_EXEC : 30, // ennyi nappal tovább is nézzük külön az elemeket
    
    results : {
        byHashtag : {},
        byDate : {},
        byYear : {},
        byYearMonth : {},
        bySb: {},
    },
    
    broncosWin : 0,
    panthersWin : 0,
    
    errorsOnFile : {},
    
    
    csvSplitter: function () {
        DB.createTableIfNotExists(DB.emptyTable);
        
        // fájl feldarabolása
        // 1.lépés : temp könyvtár létrehozása
        mkdirp(LoadCsv.tempFolder, function (err) {
            // 2. lépés : csv darabolása

            child = exec("split -l "+LoadCsv.rowsPerFile+" "+LoadCsv.filepath+" "+LoadCsv.tempFolder+"tmp", function (error, stdout, stderr) {
                if (error !== null) {
                    console.log('split exec error: ' + error);
                    return -1;
                }
                
                // 3. lépés : végigiterálás a csv fájlokon
                LoadCsv.csvRead();
            });
        });
    },
    createQuery: function (row) {
        var q = "INSERT INTO music_ (music_artist, music_name ) VALUES (";
        q += "E\'" + LoadCsv.escapeStr(row[3]) + "\', E\'" + LoadCsv.escapeStr(row[5]) + "\'";
        q += ")";
        return q;
    },
    parseCsvFile: function (file) {
        var stream = fs.createReadStream(file);
        var $this = this;
        
        var csvStream = csv.parse({delimiter: LoadCsv.csvDelimiter, quote: null, escape: null})
                .on("data", function (data) {
                    try{
                        var tweetText = data[23]; // a tweet szövege
                        var tweetDate = $this.getDateFromTwitterDateStr(data[1]);
                        var tweetDateYMD = tweetDate.yyyy_mm_dd();
                        var tweetDateY = tweetDate.getFullYear();
                        var tweetDateYm = tweetDate.yyyy_mm();
                        
                        
                        var hashTags = [],
                            pWin = false,
                            bWin = false;
                            
                        for(key in LoadCsv.HASHTAGS){
                            var toOther = false; // nem intervallumba eső dátum-e
                            if(
                                !(
                                    LoadCsv.SB_TIMES[key]['start'] < tweetDate &&
                                    LoadCsv.SB_TIMES[key]['end'] > tweetDate
                                )
                            ){
                                if(LoadCsv.SB_TIMES[key]['end'] <= tweetDate){
                                    var dL = LoadCsv.SB_TIMES[key]['end'].addDays(LoadCsv.DAYS_EXEC);
                                    if(tweetDate >= dL){
                                        toOther = true;
                                    }
                                }
                                if(!toOther){
                                    continue;
                                }
                            }
                            var isWinS = [];
                            for(kh in LoadCsv.HASHTAGS[key]){
                                for(hashtagKey in LoadCsv.HASHTAGS[key][kh]){
                                    if(tweetText.indexOf(LoadCsv.HASHTAGS[key][kh][hashtagKey]) !== -1){
                                        isWinS[key] = kh;
                                        
                                        if(typeof undefined === typeof LoadCsv.results.byHashtag[key]){
                                            LoadCsv.results.byHashtag[key] = {};
                                        }
                                        if(typeof undefined === typeof LoadCsv.results.byHashtag[key][LoadCsv.HASHTAGS[key][kh][hashtagKey]]){
                                            LoadCsv.results.byHashtag[key][LoadCsv.HASHTAGS[key][kh][hashtagKey]] = 0;
                                        }
                                        LoadCsv.results.byHashtag[key][LoadCsv.HASHTAGS[key][kh][hashtagKey]]++;
                                    }
                                }
                            }

                            if(Object.keys(isWinS).length > 0){
                                for(winHk in isWinS){
                                    
                                    //naponta
                                    if(typeof undefined === typeof LoadCsv.results.byDate[winHk]){
                                        LoadCsv.results.byDate[winHk] = {};
                                    }
                                    if(typeof undefined === typeof LoadCsv.results.byDate[winHk][tweetDateYMD]){
                                        LoadCsv.results.byDate[winHk][tweetDateYMD] = {};
                                        for(kh1 in LoadCsv.HASHTAGS[winHk]){
                                            if(typeof undefined === typeof LoadCsv.results.byDate[winHk][tweetDateYMD][kh1]){
                                                LoadCsv.results.byDate[winHk][tweetDateYMD][kh1] = 0;
                                            }
                                        }
                                    };
                                    
                                    // évente
                                    if(typeof undefined === typeof LoadCsv.results.byYear[winHk]){
                                        LoadCsv.results.byYear[winHk] = {};
                                    }
                                    if(typeof undefined === typeof LoadCsv.results.byYear[winHk][tweetDateY]){
                                        LoadCsv.results.byYear[winHk][tweetDateY] = {};
                                        for(kh1 in LoadCsv.HASHTAGS[winHk]){
                                            if(typeof undefined === typeof LoadCsv.results.byYear[winHk][tweetDateY][kh1]){
                                                LoadCsv.results.byYear[winHk][tweetDateY][kh1] = 0;
                                            }
                                        }
                                    };
                                    
                                    // havonta
                                    if(typeof undefined === typeof LoadCsv.results.byYearMonth[winHk]){
                                        LoadCsv.results.byYearMonth[winHk] = {};
                                    }
                                    if(typeof undefined === typeof LoadCsv.results.byYearMonth[winHk][tweetDateYm]){
                                        LoadCsv.results.byYearMonth[winHk][tweetDateYm] = {};
                                        for(kh1 in LoadCsv.HASHTAGS[winHk]){
                                            if(typeof undefined === typeof LoadCsv.results.byYearMonth[winHk][tweetDateYm][kh1]){
                                                LoadCsv.results.byYearMonth[winHk][tweetDateYm][kh1] = 0;
                                            }
                                        }
                                    };
                                    
                                    // tweetenként
                                    if(typeof undefined === typeof LoadCsv.results.bySb[winHk]){
                                        LoadCsv.results.bySb[winHk] = {};
                                        for(kh1 in LoadCsv.HASHTAGS[winHk]){
                                            if(typeof undefined === typeof LoadCsv.results.bySb[winHk][kh1]){
                                                LoadCsv.results.bySb[winHk][kh1] = 0;
                                            }
                                        }
                                    }
                                    
                                    if(!toOther){
                                        LoadCsv.results.byYear[winHk][tweetDateY][isWinS[winHk]]++;
                                        LoadCsv.results.bySb[winHk][isWinS[winHk]]++;
                                    }
                                    LoadCsv.results.byYearMonth[winHk][tweetDateYm][isWinS[winHk]]++;
                                    LoadCsv.results.byDate[winHk][tweetDateYMD][isWinS[winHk]]++;
                                }
                            }
                        }    
                    }catch(e){
                        if(typeof LoadCsv.errorsOnFile[file] === typeof unedfined){
                            LoadCsv.errorsOnFile[file] = [];
                        }
                        LoadCsv.errorsOnFile[file].push(data);
                    }
                    
                    
                    return;
                    /*
                    data[3] = LoadCsv.escapeStr(data[3]);
                    data[5] = LoadCsv.escapeStr(data[5]);
                    var key = data[3] + "_" + data[5];
                    LoadCsv.counter++;
                    if (!(key in LoadCsv.REQ_DATAS)) {
                        var q = LoadCsv.createQuery(data);
                        LoadCsv.REQ_DATAS[key] = key;
                        DB.insert(q);
                        if (CONSOLE_LOG) {
                            if (LoadCsv.counter % LoadCsv.rowsPerFile === 0) {
                                console.log(LoadCsv.counter + "(" + Object.keys(LoadCsv.REQ_DATAS).length + ")");
                            }
                        }
                    }
                    */
                })
                .on("end", function(){ 
                    LoadCsv.allFilesIt++;
                    var currentPercentage = LoadCsv.allFilesIt == 0 ? 0 : Math.round(LoadCsv.allFilesIt / LoadCsv.allFiles * 100);
        
                    //console.log(currentPercentage+"%");
        
                    if(currentPercentage === 100){
                        //LoadCsv.deleteTempDirectory();
                        //console.log('Az adatok betöltése elkészült!');
                        console.log("-------- EREDMÉNY ---------- ");
                       
                        console.log(LoadCsv.results);
                        
                        var t = new Date().getTime();
                        
                        fs.writeFile("./csv/results_"+t+".json", JSON.stringify(LoadCsv.results, null, 5));
                    }
                });
        stream.pipe(csvStream);
        
    },
    escapeStr: function (str) {
        return  str.replace(/\\/g, "\\\\")
                .replace(/\$/g, "\\$")
                .replace(/'/g, "\\'")
                .replace(/"/g, "\\\"");
    },
    csvRead: function (d) {
        fs.readdir(LoadCsv.tempFolder, function (err, files) {
            if (err) {
                throw err;
            }
            var fl = Object.keys(files).length;
            LoadCsv.allFiles = fl;
            files.forEach(function (file) {
                LoadCsv.parseCsvFile(LoadCsv.tempFolder + file);
            });
        });
    },
    
    deleteTempDirectory : function(){
        child = exec("rm -rf "+LoadCsv.tempFolder, function (error, stdout, stderr) {
            if (error !== null) {
                console.log('Error delete temp directory: ' + error);
                return -1;
            }
            
            console.log('temp directory deleted');
        });
    },

    getDateFromTwitterDateStr : function(d){
        var months = {
          'JAN.' : 1,
          'FEBR.' : 2,
          'MÁRC.' : 3,
          'ÁPR.' : 4,
          'MÁJ.' : 5,
          'JÚN.' : 6,
          'JÚL.' : 7,
          'AUG.' : 8,
          'SZEPT.' : 9,
          'OKT.' : 10,
          'NOV.' : 11,
          'DEC.' : 12
        };

        for(var k in months){
            if(d.indexOf(k) !== -1){
                d = d.replace(k, months[k]);
                break;
            }
        }

        d = d.split(' ').join('');
        d = d.split('-');

        var dDate = new Date('20'+''+d[0], parseInt(d[1])-1, parseInt(d[2])+1);

        return dDate;
    },
};

Date.prototype.yyyy_mm_dd = function() {
  var mm = this.getMonth() + 1; // getMonth() is zero-based
  var dd = this.getDate();

  return [this.getFullYear(), mm, dd].join('_'); // padding
};

Date.prototype.yyyy_mm = function() {
  var mm = this.getMonth() + 1; // getMonth() is zero-based
  var dd = this.getDate();

  return [this.getFullYear(), mm].join('-'); // padding
};

Date.prototype.addDays = function(days)
{
    var dat = new Date(this.valueOf());
    dat.setDate(dat.getDate() + days);
    return dat;
}