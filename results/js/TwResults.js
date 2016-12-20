var TwResults = {
  
    datas : null,
    hashtags : null,
  
    init : function(){
        $.getJSON("results2.json", function(json) {
            TwResults.datas = json;
            TwResults.createGraphs();
        });
        
        $.getJSON("../hashtags.json", function(json) {
            TwResults.hashtags = json;
            TwResults.printHashtags();
        });
        //https://twitter.com/search?q=%23SB
        
        //this.test();
    },
    
    /**
     * Hashtagek listázása
     * @returns {undefined}
     */
    printHashtags: function(){
        var $this = this;
        var hashtags = this.hashtags;
        
        $('.hashtag-list').each(function(){
           var ul = $(this);
           var k = ul.attr('data-key');
           
           for(var hk in hashtags.HASHTAGS[k]){
               $this.getHashtagLi(hashtags.HASHTAGS[k][hk]).appendTo(ul);
           }
        });
    },
    
    getHashtagLi : function(hashtag){
        var hashtagsearch = hashtag.replace("#", "%23");
        return $('<li/>').append(
              $('<a/>').attr('href', 'https://twitter.com/search?q='+hashtagsearch).html(hashtag)
              );  
    },
    
    
    
    /**
     * Grafikonok létrehozása
     * @returns {undefined}
     */
    createGraphs : function(){
        google.charts.load('current', {'packages':['corechart', 'bar']});
        
        this.createSbGraphs();
        this.createHashtagGraphs();
        this.createDayscGraphs();
    },
    
    
    /**
     * Három alapvető grafikon létrehozása, a super bowlok alapján
     */
    createSbGraphs : function(){
        var d1 = $('#graph_by_sb_2014');
        var d2 = $('#graph_by_sb_2015');
        var d3 = $('#graph_by_sb_2016');
        var index = 0;
        // Load the Visualization API and the corechart package.

        google.charts.setOnLoadCallback(function(){
            for(key in TwResults.datas.bySb){
                    var data = new google.visualization.DataTable();
                    data.addColumn('string', 'Topping');
                    data.addColumn('number', 'Slices');

                    var rows = [];

                    for(key2 in TwResults.datas.bySb[key]){
                        rows.push([
                           key2, TwResults.datas.bySb[key][key2]
                        ]);
                    }

                    data.addRows(rows);

                    // Set chart options
                    var options = {'title' : key,
                                   'width':400,
                                   'height':300};

                    var e = d1.find('div')[0];
                    if(index == 1){
                        e = d2.find('div')[0];
                    }else if(index == 2){
                        e = d3.find('div')[0];
                    }
                    
                    // Instantiate and draw our chart, passing in some options.
                    var chart = new google.visualization.PieChart(e);
                    chart.draw(data, options);
                    index++;
            }
        });
    },
    
    
    /**
     * Három alapvető grafikon létrehozása, a super bowlok alapján
     */
    createHashtagGraphs : function(){
        var d1 = $('#graph_by_hash_2014');
        var d2 = $('#graph_by_hash_2015');
        var d3 = $('#graph_by_hash_2016');
        var index = 0;
        // Load the Visualization API and the corechart package.
        
        google.charts.setOnLoadCallback(function(){
            for(key in TwResults.datas.byHashtag){
                var d = [
                    ['Hashtag', 'Használat'],
                ];
                
                for(key2 in TwResults.datas.byHashtag[key]){
                    d.push([
                       key2, TwResults.datas.byHashtag[key][key2]
                    ]);
                }


                var data = new google.visualization.arrayToDataTable(d);

                var e = d1[0];
                if(index == 1){
                    e = d2[0];
                }else if(index == 2){
                    e = d3[0];
                }

                var options = {
                  chart: {
                    title: key,
                    width : '500px',
                  },
                  bars: 'horizontal' // Required for Material Bar Charts.
                };

                var chart = new google.charts.Bar(e);
                chart.draw(data, options);
                
                index++;
            }
        });
    },
    
    /**
     * Három grafikon létrehozása, a super bowlok alapján napokra bontva
     */
    createDayscGraphs : function(){
        var d1 = $('#graph_by_year_2014');
        var d2 = $('#graph_by_year_2015');
        var d3 = $('#graph_by_year_2016');
        var index = 0;
        // Load the Visualization API and the corechart package.
        
        google.charts.setOnLoadCallback(function(){
            for(key in TwResults.datas.byYear){
                var d = [];
                
                var firstRowadded = false;
                for(key2 in TwResults.datas.byYear[key]){
                    if(firstRowadded === false){
                        var fr = ['Év'];
                        firstRowadded = Object.keys(TwResults.datas.byYear[key][key2]);
                        fr = fr.concat(firstRowadded);
                        d.push(fr);
                    }
                    var pus = [
                       key2, TwResults.datas.byYear[key][key2][firstRowadded[0]], TwResults.datas.byYear[key][key2][firstRowadded[1]],
                    ];
                    d.push(pus);
                }


                var data = new google.visualization.arrayToDataTable(d);

                var e = d1[0];
                if(index == 1){
                    e = d2[0];
                }else if(index == 2){
                    e = d3[0];
                }

                var options = {
                  chart: {
                    title: key,
                    width : '500px',
                  },
                  bars: 'vertical' // Required for Material Bar Charts.
                };

                var chart = new google.charts.Bar(e);
                chart.draw(data, options);
                
                index++;
            }
        });
    },
    
};