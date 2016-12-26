var iconv = require('iconv-lite');
var request = require('request');
var cheerio = require('cheerio');
var moment = require('moment');
var extend = require('extend');
var mysql      = require('mysql');

// node -e 'require("./index").handler()'

var options = {
    global:{
        now: moment(),
        range:{
            hour:24,
            page:10
        }
    },
    database:{
        host     : 'realbeobe.cwirqa0sfusi.ap-northeast-2.rds.amazonaws.com',
        user     : 'haks1999',
        password : 'haks2000',
        database : 'realbeobe',
        multipleStatements: true
    },
    http:{
        "HU" :{
            uri: '',
            uriWithoutPageNumber: 'http://web.humoruniv.com/board/humor/list.html?table=pds&pg=',
            startPageNumber:0,
            siteEncoding:'EUC-KR',
            method:'GET',
            encoding:null,
            headers:{
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.95 Safari/537.36"
            }
        },
        "TH" :{
            uri: '',
            uriWithoutPageNumber: 'http://www.todayhumor.co.kr/board/list.php?table=bestofbest&page=',
            startPageNumber:1,
            siteEncoding:'UTF-8',
            method:'GET',
            encoding:null,
            headers:{
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.95 Safari/537.36"
            }
        },
        "BD" :{
            uri: '',
            uriWithoutPageNumber: 'http://m.bobaedream.co.kr/board/new_writing/best/',
            startPageNumber:1,
            siteEncoding:'UTF-8',
            method:'GET',
            encoding:null,
            headers:{
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.95 Safari/537.36"
            }
        }
    }
};

var connection = {
    "DELETE": mysql.createConnection(options.database),
    "HU": mysql.createConnection(options.database),
    "TH": mysql.createConnection(options.database)
}

connection["DELETE"].connect();
connection["HU"].connect();
connection["TH"].connect();

var parseAndGetPost = {
    "HU": function($){
        var postList = [];
        $('div#cnts_list_new > div > table[class!=list_hd2] > tr').each(function(index, elemRow){
            // 광고 글은 iframe 으로 보여줌. 제외한다.
            if( $(elemRow).find('iframe').length < 1 && $(elemRow).find('td.li_num').length > 0) {
                var imageTd = $(elemRow).find('td').first();
                var titleTd = $(imageTd).next();
                var dateTd = $(titleTd).next().next();
                var viewTd = $(dateTd).next();
                var goodTd = $(viewTd).next();

                var titleA = $(titleTd).find('a').first();
                var title = $(titleA).text().replace($(titleA).find('span.list_comment_num').text(),'').replace(/[\r\t\n]/g,'');
                var link = $(titleA).attr('href');

                var site_post_id = link.split('number=')[1];
                var comment_cnt = Number($(titleA).find('span.list_comment_num').text().replace(/[^0-9]/g, ''));

                var reg_dt_date = $(dateTd).find('span.w_date').text();
                var reg_dt_time = $(dateTd).find('span.w_time').text();
                var reg_dt_moment = moment(reg_dt_date + ' ' + reg_dt_time);

                var view_cnt = Number($(viewTd).text().replace(/[^0-9]/g, ''));
                var good_cnt = Number($(goodTd).find('span.o').text().replace(/[^0-9]/g, ''));

                if( reg_dt_moment.isAfter(moment(options.global.now).subtract(options.global.range.hour, 'hours'))){
                    postList.push({
                        site_cd: "HU",
                        site_post_id: site_post_id,
                        title: title,
                        link: link,
                        comment_cnt: comment_cnt,
                        view_cnt: view_cnt,
                        good_cnt: good_cnt,
                        reg_dt_moment: reg_dt_moment
                    });
                }

            }
        });
        return postList;
    },
    "TH": function($){
        var postList = [];
        $('div.whole_box > div.vertical_container.cf > div.table_container > table > tbody > tr.view').each(function(index, elemRow){

            if( $(elemRow).find('td.list_ad').length < 1 ) {
                var idTd = $(elemRow).find('td.no').first();
                var titleTd = $(elemRow).find('td.subject').first();
                var dateTd = $(elemRow).find('td.date').first();
                var viewTd = $(elemRow).find('td.hits').first();
                var goodTd = $(elemRow).find('td.oknok').first();

                var site_post_id = $(idTd).find('a').first().text();
                var link = $(idTd).find('a').first().attr('href');
                var title = $(titleTd).find('a').first().text();
                var comment_cnt = Number($(titleTd).find('span.list_memo_count_span').text().replace(/[^0-9]/g, ''));
                var reg_dt_moment = moment($(dateTd).text(), 'YY/MM/DD HH:mm');

                var view_cnt = Number($(viewTd).text().replace(/[^0-9]/g, ''));
                var good_cnt = Number($(goodTd).text().split('/')[0].replace(/[^0-9]/g, ''));

                if( reg_dt_moment.isAfter(moment(options.global.now).subtract(options.global.range.hour, 'hours'))){
                    postList.push({
                        site_cd: "TH",
                        site_post_id: site_post_id,
                        title: title,
                        link: link,
                        comment_cnt: comment_cnt,
                        view_cnt: view_cnt,
                        good_cnt: good_cnt,
                        reg_dt_moment: reg_dt_moment
                    });
                }

            }
        });
        return postList;
    },
    "BD": function($){

        var postList = [];
        $('div.content.community ul.rank li div.info').each(function(index, elemRow){
            var mainA = $(elemRow).children('a');

            var link = $(mainA).attr('href');
            var title = $(mainA).find('div.txt span.cont').first().text();

        });
        return postList;
    }
};

var saveQuery = 'REPLACE INTO tb_m_post (site_cd, site_post_id, title, link, comment_cnt, view_cnt, good_cnt, reg_dt) ' +
    'VALUES (?, ?, ?, ?, ?, ?, ?, ?)';
var savePostList = function(args){

    if(args.postList.length < 1){
        connection[args.site_cd].end();
        return;
    }
    var saveQueryList = (function(){
        var arr = [];
        args.postList.forEach(function(post){
            var params = [post.site_cd, post.site_post_id, post.title, post.link, post.comment_cnt, post.view_cnt, post.good_cnt, post.reg_dt_moment.format('YYYY-MM-DD HH:mm:ss')];
            arr.push(mysql.format(saveQuery, params));
        });
        return arr;
    })();

    connection[args.site_cd].query(saveQueryList.join(';'), function(err, rows, results) {
        if (err){
            connection[args.site_cd].end();
            throw err;
        }
        console.log('savePostList - ', 'changed rows: ', args.site_cd, args.postList.length);
        args.nextCallFnc.apply(null, [{site_cd: args.site_cd, pageNumber: args.pageNumber+1}]);
    });
};

var deleteQuery = 'DELETE FROM tb_m_post where reg_dt < ?';
var deletePostList = function(args){

    var deleteQueryBinded = mysql.format(deleteQuery, [options.global.now.format('YYYY-MM-DD HH:mm:ss')]);
    connection["DELETE"].query(deleteQueryBinded, function(err, rows, results) {
        if (err){
            connection["DELETE"].end();
            throw err;
        }
        console.log('deletePostList - ', 'changed rows: ', rows.changedRows);
        connection["DELETE"].end();
    });
};

var callAndAnalysis = function(args){
    if(isNaN(args.pageNumber) || args.pageNumber > options.global.range.page){
        connection[args.site_cd].end();
        return;
    };

    options.http[args.site_cd].uri = options.http[args.site_cd].uriWithoutPageNumber + args.pageNumber;

    request(options.http[args.site_cd], function (error, response, html) {
        if (!error && response.statusCode == 200) {
            var strContents = new Buffer(html);
            var $ = cheerio.load(iconv.decode(strContents, options.http[args.site_cd].siteEncoding).toString());
            var postList = parseAndGetPost[args.site_cd]($);
            console.log('callAndAnalysis - ', 'targetPosts: ' , args.site_cd, postList.length);
            savePostList({site_cd:args.site_cd, pageNumber:args.pageNumber, postList:postList, nextCallFnc: callAndAnalysis});
        }
    });
};

exports.handler = function(){

    callAndAnalysis({site_cd:"HU", pageNumber:options.http["HU"].startPageNumber});
    callAndAnalysis({site_cd:"TH", pageNumber:options.http["TH"].startPageNumber});
    //callAndAnalysis({site_name:"DB", pageNumber:options.http["DB"].startPageNumber});
    deletePostList();

};




