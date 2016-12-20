var AWS = require("aws-sdk");
var iconv = require('iconv-lite');
var request = require('request');
var cheerio = require('cheerio');
var moment = require('moment');

AWS.config.update({
    region: "ap-northeast-2",
    endpoint: "dynamodb.ap-northeast-2.amazonaws.com"
});
// node -e 'require("./index").handler()'

var docClient = new AWS.DynamoDB.DocumentClient();

var options = {
    global:{
        now: moment(),
        range:{
            days: 3
        }
    },
    http:{
        "humoruniv" :{
            uri: '',
            uriWithoutPageNumber: 'http://web.humoruniv.com/board/humor/list.html?table=pds&pg=',
            method:'GET',
            encoding:null,
            headers:{
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.95 Safari/537.36"
            }
        }
    }

};

var createPostBatchRequest = function(postList){

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

var createPost =  function(post){
    return {
        TableName: 'best_post',
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
            reg_dt: {
                Action: "PUT",
                Value: post.reg_dt
            },
            title: {
                Action: "PUT",
                Value: post.title
            },
            link: {
                Action: "PUT",
                Value: post.link
            }
        },
        Item: {
            site_name: post.site_name,
            site_post_id: post.site_post_id,
            view_cnt: post.view_cnt,
            comment_cnt: post.comment_cnt,
            good_cnt: post.good_cnt,
            reg_dt: post.reg_dt,
            title: post.title,
            link: post.link
        }
    };
};

var parseAndSave = {
    "humoruniv": function($){
        var postList = [];
        var isLast = false;
        $('div#cnts_list_new > div > table[class!=list_hd2] > tr').each(function(index, elemRow){
            // 광고 글은 iframe 으로 보여줌. 제외한다.
            if( $(elemRow).find('iframe').length < 1 && $(elemRow).find('td.li_num').length > 0) {
                var imageTd = $(elemRow).find('td').first();
                var titleTd = $(imageTd).next();
                var dateTd = $(titleTd).next().next();
                var viewTd = $(dateTd).next();
                var goodTd = $(viewTd).next();

                var titleA = $(titleTd).find('a').first();
                var title = $(titleA).text();
                var link = $(titleA).attr('href');

                var site_post_id = link.split('number=')[1];
                var comment_cnt = Number($(titleA).find('span.list_comment_num').text().replace(/[^0-9]/g, ''));

                var reg_dt_date = $(dateTd).find('span.w_date').text();
                var reg_dt_time = $(dateTd).find('span.w_time').text();
                var reg_dt_moment = moment(reg_dt_date + ' ' + reg_dt_time);

                var view_cnt = Number($(viewTd).text().replace(/[^0-9]/g, ''));
                var good_cnt = Number($(goodTd).find('span.o').text().replace(/[^0-9]/g, ''));

                isLast = reg_dt_moment.isBefore(moment(options.global.now).subtract(options.global.range.days, 'days'));

                if( isLast ){
                    var post = createPost({site_name :"humoruniv", site_post_id:site_post_id, title:title, link:link,
                        comment_cnt:comment_cnt, view_cnt:view_cnt, good_cnt:good_cnt, reg_dt:reg_dt_moment.toString()})
                    postList.push(post);
                }
            }
        });

        docClient.batchWrite(createPostBatchRequest(postList), function (err, data) {
            if (err) {
                console.error("Unable to add item. Error JSON:", JSON.stringify(err, null, 2));
            } else {
                console.log("Added item:", JSON.stringify(data));
            }
        });
    }
};

var callAndAnalysis  = {
    "humoruniv": function(){
        var pageNumber = 0;
        while(pageNumber < 10){
            options.http["humoruniv"].uri = options.http["humoruniv"].uriWithoutPageNumber + pageNumber++;
            request(options.http["humoruniv"], function (error, response, html) {
                if (!error && response.statusCode == 200) {
                    var strContents = new Buffer(html);
                    var $ = cheerio.load(iconv.decode(strContents, 'EUC-KR').toString());
                    parseAndSave["humoruniv"]($);
                }
            });
        }

    }
};

exports.handler = function(){

    callAndAnalysis["humoruniv"]();

};





