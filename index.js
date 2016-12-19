var AWS = require("aws-sdk");
var iconv = require('iconv-lite');
var request = require('request');
var cheerio = require('cheerio');

AWS.config.update({
    region: "ap-northeast-2",
    endpoint: "dynamodb.ap-northeast-2.amazonaws.com"
});
// node -e 'require("./index").handler()'

var docClient = new AWS.DynamoDB.DocumentClient();

var options = {
    uri: 'http://m.humoruniv.com/board/humor/list.html?table=pds',
    method:'GET',
    encoding:null,
    headers:{
       'User-Agent':'Mozilla/5.0 (Linux; Android 5.0; SM-G900P Build/LRX21T) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.95 Mobile Safari/537.36'
    }
};



exports.handler = function(){

    request(options, function (error, response, html) {

        var strContents = new Buffer(html);
        //console.log(iconv.decode(strContents, 'EUC-KR').toString());

        var $ = cheerio.load(iconv.decode(strContents, 'EUC-KR').toString());

        $('#list_body > ul > a').each(function(index, elemA){

            console.log($(elemA).attr('href'));
            console.log($(elemA).find('table td dd span').first().text());
            console.log($(elemA).find('span[class=ok_num]').first().text());
            console.log($(elemA).find('span[class=not_ok_num]').first().text());
            console.log($(elemA).find('span[class=comment_num]').first().text());
            console.log($(elemA).find('span[class=extra]').first().text());


            var board = {
                TableName: 'best_board',
                Item:{
                    site_name:'humoruniv',
                    read_cnt:Number($(elemA).find('span[class=extra]').first().text().replace(/[^0-9]/g, '')),
                    title: $(elemA).find('table td dd span').first().text(),
                    link:$(elemA).attr('href')
                }
            };

            docClient.put(board, function(err, data) {
                if (err) {
                    console.error("Unable to add item. Error JSON:", JSON.stringify(err, null, 2));
                } else {
                    console.log("Added item:", JSON.stringify(data, null, 2));
                }
            });
        });

        //console.log(response.headers['content-type']);

        //var data = iconv.convert(html).toString('UTF-8');

        //response.setHeader('Content-type','text/html;charset=utf-8')

        //console.log( html );
        //console.log(response.statusCode);
        //console.log(html);
        if (!error && response.statusCode == 200) {
            //console.log(html);
        }
    });


};
