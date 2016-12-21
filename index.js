var AWS = require("aws-sdk");
var iconv = require('iconv-lite');
var request = require('request');
var cheerio = require('cheerio');
var moment = require('moment');
var extend = require('extend');

const uuidV1 = require('uuid/v1');

AWS.config.update({
    region: "ap-northeast-2",
    endpoint: "dynamodb.ap-northeast-2.amazonaws.com"
});
// node -e 'require("./index").batchCreate()'
// node -e 'require("./index").batchDelete()'

var docClient = new AWS.DynamoDB.DocumentClient();

var options = {
    global:{
        now: moment(),
        range:{
            hour:24,
            page:10
        }
    },
    db:{
        batchSize: 25 , //dynamodb 배치 한 번에 최대 25개까지만 지원함
        delay: 200    // 재귀로 막 넣으니 오류 없이 데이터 누락생김. capacity 때문임 ㅠㅠ. 일단 조금씩 하자
    },
    http:{
        "humoruniv" :{
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
        "todayhumor" :{
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
        "bobaedream" :{
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


var tableSchema = {
    "best_post":{
        TableName: 'best_post'
    }
};

var generateCreatePostBatchRequest = function(postList){

    var postPutRequestList = [];
    postList.forEach(function(data){
        postPutRequestList.push({
           "PutRequest": data
        });
    });

    return {
        RequestItems : {
            "best_post": postPutRequestList
        }
    }
};

var generateCreatePostRequest =  function(post){
    return extend({
        Key: {
            site_name: post.site_name,
            site_post_id: post.site_post_id
        },
        AttributeUpdates: {
            view_cnt: {
                Action: "PUT",
                Value: post.view_cnt
            },
            comment_cnt: {
                Action: "PUT",
                Value: post.comment_cnt
            },
            good_cnt: {
                Action: "PUT",
                Value: post.good_cnt
            },
            title: {
                Action: "PUT",
                Value: post.title
            }
        },
        Item: {
            post_id: uuidV1(),
            site_name: post.site_name,
            site_post_id: post.site_post_id,
            view_cnt: post.view_cnt,
            comment_cnt: post.comment_cnt,
            good_cnt: post.good_cnt,
            reg_dt: post.reg_dt,
            reg_millis: post.reg_millis,
            title: post.title,
            link: post.link
        }
    },tableSchema["best_post"]);
};

var generateDeletePostBatchRequest = function(postList){
    var postDeleteRequestList = [];
    postList.forEach(function(data){
        postDeleteRequestList.push({
            "DeleteRequest": data
        });
    });

    return {
        RequestItems : {
            "best_post": postDeleteRequestList
        }
    }
};

var generateDeletePostRequest = function(site_name, site_post_id){
    return extend({
        Key: {
            site_name: site_name,
            site_post_id: site_post_id
        }
    }, tableSchema["best_post"]);
};

var generateFindPostForDeleteRequest = function(site_name){
    return extend({
        IndexName: "site_name-reg_millis-index",
        KeyConditionExpression: "site_name = :site_name and reg_millis < :range",
        ExpressionAttributeValues: {
            ":site_name" : site_name,
            ":range": moment(options.global.now).subtract(options.global.range.hour, 'minutes').valueOf()
        }
    }, tableSchema["best_post"]);
};

var addCreatePostRequestToList = function(postList, args){
    var outOfRange = args.reg_dt_moment.isBefore(moment(options.global.now).subtract(options.global.range.hour, 'hours'));

    if( !outOfRange ){
        var post = generateCreatePostRequest({
            site_name: args.site_name,
            site_post_id: args.site_post_id,
            title: args.title,
            link: args.link,
            comment_cnt: args.comment_cnt,
            view_cnt: args.view_cnt,
            good_cnt: args.good_cnt,
            reg_dt: args.reg_dt_moment.toString(),
            reg_millis: args.reg_dt_moment.valueOf()})

        postList.push(post);
    }
};

var parseAndGetPost = {
    "humoruniv": function($){
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

                addCreatePostRequestToList(postList, {
                    site_name: "humoruniv",
                    site_post_id: site_post_id,
                    title: title,
                    link: link,
                    comment_cnt: comment_cnt,
                    view_cnt: view_cnt,
                    good_cnt: good_cnt,
                    reg_dt_moment: reg_dt_moment
                });
            }
        });
        return postList;
    },
    "todayhumor": function($){
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

                addCreatePostRequestToList(postList, {
                    site_name: "todayhumor",
                    site_post_id: site_post_id,
                    title: title,
                    link: link,
                    comment_cnt: comment_cnt,
                    view_cnt: view_cnt,
                    good_cnt: good_cnt,
                    reg_dt_moment: reg_dt_moment
                });
            }
        });
        return postList;
    },
    "bobaedream": function($){

        var postList = [];
        $('div.content.community ul.rank li div.info').each(function(index, elemRow){
            var mainA = $(elemRow).children('a');

            var link = $(mainA).attr('href');
            var title = $(mainA).find('div.txt span.cont').first().text();

        });
        return postList;
    }
};

var saveDocument = function(args){

    setTimeout(function(args){
        var currentTargetPostList = [];
        var restTargetPostList = [];
        if( args.postList.length > options.db.batchSize ){
            currentTargetPostList = args.postList.slice(0, options.db.batchSize);
            restTargetPostList = args.postList.slice(options.db.batchSize-1);
        }else{
            currentTargetPostList = args.postList;
        }

        var batchRequest = generateCreatePostBatchRequest(currentTargetPostList);

        docClient.batchWrite(batchRequest, function (err, data) {
            if (err) {
                console.error("Unable to add item. Error JSON:", JSON.stringify(err, null, 2));
            } else {
                console.log("saveDocument ", args.site_name, currentTargetPostList.length, restTargetPostList.length);
                if( restTargetPostList.length < 1 ){
                    if( args.nextCallFnc ){
                        args.nextCallFnc.apply(null, [{site_name: args.site_name, pageNumber: args.pageNumber+1}]);
                    }
                }else{
                    saveDocument({site_name: args.site_name, pageNumber:args.pageNumber, postList:restTargetPostList, nextCallFnc:args.nextCallFnc});
                }
            }
        });
    }, options.db.delay, args);

};


var deleteDocument = function(args){

    setTimeout(function(args){
        var currentTargetPostList = [];
        var restTargetPostList = [];
        if( args.postList.length > options.db.batchSize ){
            currentTargetPostList = args.postList.slice(0, options.db.batchSize);
            restTargetPostList = args.postList.slice(options.db.batchSize-1);
        }else{
            currentTargetPostList = args.postList;
        }

        var batchRequest = generateDeletePostBatchRequest(currentTargetPostList);

        docClient.batchWrite(batchRequest, function (err, data) {
            if (err) {
                console.error("Unable to add item. Error JSON:", JSON.stringify(err, null, 2));
            } else {
                console.log("deleteDocument ", args.site_name, currentTargetPostList.length, restTargetPostList.length);
                if(restTargetPostList.length > 0){
                    deleteDocument({site_name: args.site_name, postList:restTargetPostList});
                }
            }
        });
    }, options.db.delay, args);

};

// var deleteDocument = function(args){
//
//     if(args.postList.length < 1) return;
//
//     setTimeout(function(args){
//
//         docClient.delete(args.postList.pop(), function (err, data) {
//             if (err) {
//                 console.error("Unable to add item. Error JSON:", JSON.stringify(err, null, 2));
//             } else {
//                 console.log("deleteDocument ", args.site_name, args.postList.length);
//                 deleteDocument({site_name: args.site_name, postList:args.postList});
//             }
//         });
//     }, 500, args);
//
// };

var callAndAnalysis = function(args){
    if(isNaN(args.pageNumber) || args.pageNumber > options.global.range.page) return;

    options.http[args.site_name].uri = options.http[args.site_name].uriWithoutPageNumber + args.pageNumber;

    request(options.http[args.site_name], function (error, response, html) {
        if (!error && response.statusCode == 200) {
            var strContents = new Buffer(html);
            var $ = cheerio.load(iconv.decode(strContents, options.http[args.site_name].siteEncoding).toString());
            var postList = parseAndGetPost[args.site_name]($);
            if( postList && postList.length > 1){
                saveDocument({site_name:args.site_name, pageNumber:args.pageNumber, postList:postList, type: 'put', nextCallFnc: callAndAnalysis});
            }
        }
    });
};

var findAndDelete = function(args){

    docClient.query(generateFindPostForDeleteRequest(args.site_name), function(err, data) {
        if (err) {
            console.log(JSON.stringify(err, null, 2));
        } else {
            var postList = [];
            data.Items.forEach(function(item){
                postList.push(generateDeletePostRequest(item.site_name, item.site_post_id));
            });

            if( postList.length > 1){
                deleteDocument({site_name:args.site_name, postList:postList});
            }
        }
    });

};

exports.handler = function(){
    callAndAnalysis({site_name:"humoruniv", pageNumber:options.http["humoruniv"].startPageNumber});
};

exports.batchCreate = function(){
    //callAndAnalysis({site_name:"humoruniv", pageNumber:options.http["humoruniv"].startPageNumber});
    //callAndAnalysis({site_name:"todayhumor", pageNumber:options.http["todayhumor"].startPageNumber});
    callAndAnalysis({site_name:"bobaedream", pageNumber:options.http["bobaedream"].startPageNumber});
};

exports.batchDelete = function(){

    //findAndDelete({site_name:"humoruniv"});
    //findAndDelete({site_name:"todayhumor"});
    findAndDelete({site_name:"bobaedream"});


};




