var fs = require('fs');
var vm = require('vm')
var csv = require('fast-csv');
var sys = require('sys')
var exec = require('child_process').exec;
var child;
var path = require("path");
var mkdirp = require('mkdirp');
var CONSOLE_LOG = true;


var rmdir = function(dir) {
    var list = fs.readdirSync(dir);
    for(var i = 0; i < list.length; i++) {
        var filename = path.join(dir, list[i]);
        var stat = fs.statSync(filename);

        if(filename == "." || filename == "..") {
            // pass these files
        } else if(stat.isDirectory()) {
            // rmdir recursively
            rmdir(filename);
        } else {
            // rm fiilename
            fs.unlinkSync(filename);
        }
    }
    fs.rmdirSync(dir);
};

TwitterSBanalitycs = {

    REQ_DATAS: [],
    currentPercentage: 0,
    allFilesIt: 0,
    allFilesLength: 0,
    counter: 0,

    // TODO: itt majd meg kell adni a super bowl adathalmazt a daraboláshoz
    filepath: './csv/superbowl_all.csv',
    tempFolder: './csv/temp/',
    csvDelimiter: '|',
    rowsPerFile: 10000, // milyen sorközönként vágja szét a nagyobb fájlt

    SB_TIMES: {},

    HASHTAGS_JSON: {}, // ebbe az objektumba kerül bele a kereséshez szükséges adat, pl a hashtegek

    DAYS_EXEC: 30, // ennyi nappal tovább is nézzük külön az elemeket

    results: {
        byHashtag: {},
        byDate: {},
        byYear: {},
        byYearMonth: {},
        bySb: {},
    },

    errorsOnFile: {},

    /*
     * A függvény segítségévél feldaraboljuk a csv fájlt több különböző darabra
     * @returns {undefined}
     */
    csvSplitter: function () {
        // fájl feldarabolása

        // 0.lépés : temp könyvtár törlése
        //rmdir(TwitterSBanalitycs.tempFolder);

        // 1.lépés : temp könyvtár létrehozása
        mkdirp(TwitterSBanalitycs.tempFolder, function (err) {
            // 2. lépés : csv darabolása

            child = exec("split -l " + TwitterSBanalitycs.rowsPerFile + " " + TwitterSBanalitycs.filepath + " " + TwitterSBanalitycs.tempFolder + "tmp", function (error, stdout, stderr) {
                if (error !== null) {
                    console.log('split exec error: ' + error);
                    return -1;
                }

                // 3. lépés : végigiterálás a csv fájlokon
                TwitterSBanalitycs.csvRead();
            });
        });
    },
    parseCsvFile: function (file) {
        var stream = fs.createReadStream(file);
        var $this = this;

        var csvStream = csv.parse({delimiter: TwitterSBanalitycs.csvDelimiter, quote: null, escape: null})
                .on("data", function (data) {
                    try {
                        var tweetText = data[23]; // a tweet szövege
                        var tweetDate = $this.getDateFromTwitterDateStr(data[1]);


                        for (var year0 in TwitterSBanalitycs.HASHTAGS_JSON.TEAMS) {
                            var key = "SB" + year0;  // key = SB2014... stb
                            TwitterSBanalitycs.setDataToAnalytics(year0, tweetText, tweetDate, TwitterSBanalitycs.SB_TIMES[key]['start'], TwitterSBanalitycs.SB_TIMES[key]['end']);
                        }
                    } catch (e) {
                        if (typeof TwitterSBanalitycs.errorsOnFile[file] === typeof unedfined) {
                            TwitterSBanalitycs.errorsOnFile[file] = [];
                        }
                        TwitterSBanalitycs.errorsOnFile[file].push(data);
                    }
                })
                .on("end", function () {

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
        this.initSbTimes();

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

    /**
     * Dátumokat tároló objektum inicializálása
     * Az ebben tárolt időszakokat fogjuk figyelni az adatgyűjtés során
     * @returns {undefined}
     */
    initSbTimes: function () {
        for (var year in TwitterSBanalitycs.HASHTAGS_JSON.SB_TIMES) {
            if (year === '_comment') {
                continue;
            }
            var dstr = TwitterSBanalitycs.HASHTAGS_JSON.SB_TIMES[year];

            var d1 = new Date(dstr);
            var d2 = new Date(dstr);
            d1.setDate(d1.getDate() - 1);
            d2.setDate(d1.getDate() - 90);

            TwitterSBanalitycs.SB_TIMES['SB' + year] = {
                'start': d2,
                'end': d1,
            };
        }
    },

    deleteTempDirectory: function () {
        child = exec("rm -rf " + TwitterSBanalitycs.tempFolder, function (error, stdout, stderr) {
            if (error !== null) {
                console.log('Error delete temp directory: ' + error);
                return -1;
            }

            console.log('temp directory deleted');
        });
    },

    getDateFromTwitterDateStr: function (d) {
        var months = {
            'JAN.': 1,
            'FEBR.': 2,
            'MÁRC.': 3,
            'ÁPR.': 4,
            'MÁJ.': 5,
            'JÚN.': 6,
            'JÚL.': 7,
            'AUG.': 8,
            'SZEPT.': 9,
            'OKT.': 10,
            'NOV.': 11,
            'DEC.': 12
        };

        for (var k in months) {
            if (d.indexOf(k) !== -1) {
                d = d.replace(k, months[k]);
                break;
            }
        }

        d = d.split(' ').join('');
        d = d.split('-');

        var dDate = new Date('20' + '' + d[0], parseInt(d[1]) - 1, parseInt(d[2]) + 1);

        return dDate;
    },

    /*
     * adatok vizsgálása és tömbbe rendelése
     * Egyetlen tweet-et dolgoz vel és a megfelelő tömbökbe pakolja bele
     */
    setDataToAnalytics: function (year0, tweetText, tweetDate, dateStart, dateEnd) {
        var key = "SB" + year0;  // key = SB2014... stb
        var toOther = false; // nem intervallumba eső dátum-e
        var noContinue = false;
        if (
                dateStart && dateEnd &&
                !(
                        dateStart < tweetDate &&
                        dateEnd > tweetDate
                        )
                ) {
            if (dateEnd <= tweetDate) {
                var dL = dateEnd.addDays(TwitterSBanalitycs.DAYS_EXEC);
                if (tweetDate >= dL) {
                    toOther = true;
                }
            }
            if (!toOther) {
                noContinue = true;
            }
        }
        if (!noContinue) {
            var tweetDateYMD = tweetDate.yyyy_mm_dd();
            var tweetDateY = tweetDate.getFullYear();
            var tweetDateYm = tweetDate.yyyy_mm();
            var isWinS = [];
            var hashtagData = {};
            hashtagData[TwitterSBanalitycs.HASHTAGS_JSON.TEAMS[year0].W] = TwitterSBanalitycs.HASHTAGS_JSON.HASHTAGS[TwitterSBanalitycs.HASHTAGS_JSON.TEAMS[year0].W];
            hashtagData[TwitterSBanalitycs.HASHTAGS_JSON.TEAMS[year0].L] = TwitterSBanalitycs.HASHTAGS_JSON.HASHTAGS[TwitterSBanalitycs.HASHTAGS_JSON.TEAMS[year0].L];

            for (kh in hashtagData) {
                for (hashtagKey in hashtagData[kh]) {
                    if (tweetText.indexOf(hashtagData[kh][hashtagKey]) !== -1) {
                        isWinS[key] = kh;

                        if (typeof undefined === typeof TwitterSBanalitycs.results.byHashtag[key]) {
                            TwitterSBanalitycs.results.byHashtag[key] = {};
                        }
                        if (typeof undefined === typeof TwitterSBanalitycs.results.byHashtag[key][hashtagData[kh][hashtagKey]]) {
                            TwitterSBanalitycs.results.byHashtag[key][hashtagData[kh][hashtagKey]] = 0;
                        }
                        TwitterSBanalitycs.results.byHashtag[key][hashtagData[kh][hashtagKey]]++;
                    }
                }
            }

            if (Object.keys(isWinS).length > 0) {
                for (winHk in isWinS) {
                    //naponta
                    if (typeof undefined === typeof TwitterSBanalitycs.results.byDate[winHk]) {
                        TwitterSBanalitycs.results.byDate[winHk] = {};
                    }
                    if (typeof undefined === typeof TwitterSBanalitycs.results.byDate[winHk][tweetDateYMD]) {
                        TwitterSBanalitycs.results.byDate[winHk][tweetDateYMD] = {};
                        for (kh1 in hashtagData) {
                            if (typeof undefined === typeof TwitterSBanalitycs.results.byDate[winHk][tweetDateYMD][kh1]) {
                                TwitterSBanalitycs.results.byDate[winHk][tweetDateYMD][kh1] = 0;
                            }
                        }
                    }


                    // évente
                    if (typeof undefined === typeof TwitterSBanalitycs.results.byYear[winHk]) {
                        TwitterSBanalitycs.results.byYear[winHk] = {};
                    }
                    if (typeof undefined === typeof TwitterSBanalitycs.results.byYear[winHk][tweetDateY]) {
                        TwitterSBanalitycs.results.byYear[winHk][tweetDateY] = {};
                        for (kh1 in hashtagData) {
                            if (typeof undefined === typeof TwitterSBanalitycs.results.byYear[winHk][tweetDateY][kh1]) {
                                TwitterSBanalitycs.results.byYear[winHk][tweetDateY][kh1] = 0;
                            }
                        }
                    }


                    // havonta
                    if (typeof undefined === typeof TwitterSBanalitycs.results.byYearMonth[winHk]) {
                        TwitterSBanalitycs.results.byYearMonth[winHk] = {};
                    }
                    if (typeof undefined === typeof TwitterSBanalitycs.results.byYearMonth[winHk][tweetDateYm]) {
                        TwitterSBanalitycs.results.byYearMonth[winHk][tweetDateYm] = {};
                        for (kh1 in hashtagData) {
                            if (typeof undefined === typeof TwitterSBanalitycs.results.byYearMonth[winHk][tweetDateYm][kh1]) {
                                TwitterSBanalitycs.results.byYearMonth[winHk][tweetDateYm][kh1] = 0;
                            }
                        }
                    }


                    // tweetenként
                    if (typeof undefined === typeof TwitterSBanalitycs.results.bySb[winHk]) {
                        TwitterSBanalitycs.results.bySb[winHk] = {};
                        for (kh1 in hashtagData) {
                            if (typeof undefined === typeof TwitterSBanalitycs.results.bySb[winHk][kh1]) {
                                TwitterSBanalitycs.results.bySb[winHk][kh1] = 0;
                            }
                        }
                    }

                    if (!toOther) {
                        TwitterSBanalitycs.results.byYear[winHk][tweetDateY][isWinS[winHk]]++;
                        TwitterSBanalitycs.results.bySb[winHk][isWinS[winHk]]++;
                    }
                    TwitterSBanalitycs.results.byYearMonth[winHk][tweetDateYm][isWinS[winHk]]++;
                    TwitterSBanalitycs.results.byDate[winHk][tweetDateYMD][isWinS[winHk]]++;
                }
            }
        }
    },
};

Date.prototype.yyyy_mm_dd = function () {
    var mm = this.getMonth() + 1; // getMonth() is zero-based
    var dd = this.getDate();

    return [this.getFullYear(), mm, dd].join('_'); // padding
};

Date.prototype.yyyy_mm = function () {
    var mm = this.getMonth() + 1; // getMonth() is zero-based
    var dd = this.getDate();

    return [this.getFullYear(), mm].join('-'); // padding
};

Date.prototype.addDays = function (days)
{
    var dat = new Date(this.valueOf());
    dat.setDate(dat.getDate() + days);
    return dat;
}