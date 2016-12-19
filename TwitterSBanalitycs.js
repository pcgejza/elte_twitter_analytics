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

TwitterSBanalitycs = {
    
    
    REQ_DATAS : [],
    currentPercentage : 0,
    allFilesIt  : 0,
    allFilesLength : 0,
    counter : 0,
    
    
    // TODO: itt majd meg kell adni a super bowl adathalmazt a daraboláshoz
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
   
    
    HASHTAGS_JSON : {}, // ebbe az objektumba kerül bele a kereséshez szükséges adat, pl a hashtegek
    
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
        mkdirp(TwitterSBanalitycs.tempFolder, function (err) {
            // 2. lépés : csv darabolása

            child = exec("split -l "+TwitterSBanalitycs.rowsPerFile+" "+TwitterSBanalitycs.filepath+" "+TwitterSBanalitycs.tempFolder+"tmp", function (error, stdout, stderr) {
                if (error !== null) {
                    console.log('split exec error: ' + error);
                    return -1;
                }
                
                // 3. lépés : végigiterálás a csv fájlokon
                TwitterSBanalitycs.csvRead();
            });
        });
    },
    createQuery: function (row) {
        var q = "INSERT INTO music_ (music_artist, music_name ) VALUES (";
        q += "E\'" + TwitterSBanalitycs.escapeStr(row[3]) + "\', E\'" + TwitterSBanalitycs.escapeStr(row[5]) + "\'";
        q += ")";
        return q;
    },
    parseCsvFile: function (file) {
        var stream = fs.createReadStream(file);
        var $this = this;
        
        var csvStream = csv.parse({delimiter: TwitterSBanalitycs.csvDelimiter, quote: null, escape: null})
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
                            
                            
                        for(var year0 in TwitterSBanalitycs.HASHTAGS_JSON.TEAMS){
                            var key = "SB"+year0;  // key = SB2014... stb
                            var toOther = false; // nem intervallumba eső dátum-e
                            if(
                                !(
                                    TwitterSBanalitycs.SB_TIMES[key]['start'] < tweetDate &&
                                    TwitterSBanalitycs.SB_TIMES[key]['end'] > tweetDate
                                )
                            ){
                                if(TwitterSBanalitycs.SB_TIMES[key]['end'] <= tweetDate){
                                    var dL = TwitterSBanalitycs.SB_TIMES[key]['end'].addDays(TwitterSBanalitycs.DAYS_EXEC);
                                    if(tweetDate >= dL){
                                        toOther = true;
                                    }
                                }
                                if(!toOther){
                                    continue;
                                }
                            }
                            var isWinS = [];
                            
                            var hashtagData = { };
                            hashtagData[TwitterSBanalitycs.HASHTAGS_JSON.TEAMS[year0].W] = TwitterSBanalitycs.HASHTAGS_JSON.HASHTAGS[TwitterSBanalitycs.HASHTAGS_JSON.TEAMS[year0].W];
                            hashtagData[TwitterSBanalitycs.HASHTAGS_JSON.TEAMS[year0].L] = TwitterSBanalitycs.HASHTAGS_JSON.HASHTAGS[TwitterSBanalitycs.HASHTAGS_JSON.TEAMS[year0].L];
                            
                            for(kh in hashtagData){
                                for(hashtagKey in hashtagData[kh]){
                                    if(tweetText.indexOf(hashtagData[kh][hashtagKey]) !== -1){ 
                                        isWinS[key] = kh;
                                        
                                        if(typeof undefined === typeof TwitterSBanalitycs.results.byHashtag[key]){
                                            TwitterSBanalitycs.results.byHashtag[key] = {};
                                        }
                                        if(typeof undefined === typeof TwitterSBanalitycs.results.byHashtag[key][hashtagData[kh][hashtagKey]]){
                                            TwitterSBanalitycs.results.byHashtag[key][hashtagData[kh][hashtagKey]] = 0;
                                        }
                                        TwitterSBanalitycs.results.byHashtag[key][hashtagData[kh][hashtagKey]]++;
                                    }
                                }
                            }

                            if(Object.keys(isWinS).length > 0){
                                for(winHk in isWinS){
                                    //naponta
                                    if(typeof undefined === typeof TwitterSBanalitycs.results.byDate[winHk]){
                                        TwitterSBanalitycs.results.byDate[winHk] = {};
                                    }
                                    if(typeof undefined === typeof TwitterSBanalitycs.results.byDate[winHk][tweetDateYMD]){
                                        TwitterSBanalitycs.results.byDate[winHk][tweetDateYMD] = {};
                                        for(kh1 in hashtagData){
                                            if(typeof undefined === typeof TwitterSBanalitycs.results.byDate[winHk][tweetDateYMD][kh1]){
                                                TwitterSBanalitycs.results.byDate[winHk][tweetDateYMD][kh1] = 0;
                                            }
                                        }
                                    };
                                    
                                    // évente
                                    if(typeof undefined === typeof TwitterSBanalitycs.results.byYear[winHk]){
                                        TwitterSBanalitycs.results.byYear[winHk] = {};
                                    }
                                    if(typeof undefined === typeof TwitterSBanalitycs.results.byYear[winHk][tweetDateY]){
                                        TwitterSBanalitycs.results.byYear[winHk][tweetDateY] = {};
                                        for(kh1 in hashtagData){
                                            if(typeof undefined === typeof TwitterSBanalitycs.results.byYear[winHk][tweetDateY][kh1]){
                                                TwitterSBanalitycs.results.byYear[winHk][tweetDateY][kh1] = 0;
                                            }
                                        }
                                    };
                                    
                                    // havonta
                                    if(typeof undefined === typeof TwitterSBanalitycs.results.byYearMonth[winHk]){
                                        TwitterSBanalitycs.results.byYearMonth[winHk] = {};
                                    }
                                    if(typeof undefined === typeof TwitterSBanalitycs.results.byYearMonth[winHk][tweetDateYm]){
                                        TwitterSBanalitycs.results.byYearMonth[winHk][tweetDateYm] = {};
                                        for(kh1 in hashtagData){
                                            if(typeof undefined === typeof TwitterSBanalitycs.results.byYearMonth[winHk][tweetDateYm][kh1]){
                                                TwitterSBanalitycs.results.byYearMonth[winHk][tweetDateYm][kh1] = 0;
                                            }
                                        }
                                    };
                                    
                                    // tweetenként
                                    if(typeof undefined === typeof TwitterSBanalitycs.results.bySb[winHk]){
                                        TwitterSBanalitycs.results.bySb[winHk] = {};
                                        for(kh1 in hashtagData){
                                            if(typeof undefined === typeof TwitterSBanalitycs.results.bySb[winHk][kh1]){
                                                TwitterSBanalitycs.results.bySb[winHk][kh1] = 0;
                                            }
                                        }
                                    }
                                    
                                    if(!toOther){
                                        TwitterSBanalitycs.results.byYear[winHk][tweetDateY][isWinS[winHk]]++;
                                        TwitterSBanalitycs.results.bySb[winHk][isWinS[winHk]]++;
                                    }
                                    TwitterSBanalitycs.results.byYearMonth[winHk][tweetDateYm][isWinS[winHk]]++;
                                    TwitterSBanalitycs.results.byDate[winHk][tweetDateYMD][isWinS[winHk]]++;
                                }
                            }
                        }    
                    }catch(e){
                        if(typeof TwitterSBanalitycs.errorsOnFile[file] === typeof unedfined){
                            TwitterSBanalitycs.errorsOnFile[file] = [];
                        }
                        TwitterSBanalitycs.errorsOnFile[file].push(data);
                    }
                })
                .on("end", function(){ 
                    TwitterSBanalitycs.allFilesIt++;
                    var currentPercentage = TwitterSBanalitycs.allFilesIt == 0 ? 0 : Math.round(TwitterSBanalitycs.allFilesIt / TwitterSBanalitycs.allFiles * 100);
        
                    //console.log(currentPercentage+"%");
        
                    if(currentPercentage === 100){
                        //TwitterSBanalitycs.deleteTempDirectory();
                        //console.log('Az adatok betöltése elkészült!');
                        console.log("-------- EREDMÉNY ---------- ");
                       
                        console.log(TwitterSBanalitycs.results);
                        
                        var t = new Date().getTime();
                        
                        fs.writeFile("./csv/results_"+t+".json", JSON.stringify(TwitterSBanalitycs.results, null, 5));
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
        this.HASHTAGS_JSON = JSON.parse(fs.readFileSync('hashtags.json', 'utf8'));
        
        fs.readdir(TwitterSBanalitycs.tempFolder, function (err, files) {
            if (err) {
                throw err;
            }
            var fl = Object.keys(files).length;
            TwitterSBanalitycs.allFiles = fl;
            files.forEach(function (file) {
                TwitterSBanalitycs.parseCsvFile(TwitterSBanalitycs.tempFolder + file);
            });
        });
    },
    
    deleteTempDirectory : function(){
        child = exec("rm -rf "+TwitterSBanalitycs.tempFolder, function (error, stdout, stderr) {
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