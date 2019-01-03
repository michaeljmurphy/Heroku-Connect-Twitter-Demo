var Twitter = require('twitter');
var pg = require('pg');

var tw = new Twitter({
    consumer_key: process.env.TWITTER_CONSUMER_KEY,
    consumer_secret: process.env.TWITTER_CONSUMER_SECRET,
    access_token_key: process.env.TWITTER_ACCESS_TOKEN_KEY,
    access_token_secret: process.env.TWITTER_ACCESS_TOKEN_SECRET,
});

console.log("Connecting to pg", process.env.HEROKU_POSTGRESQL_BRONZE_URL);

pg.connect(process.env.HEROKU_POSTGRESQL_BRONZE_URL+'?ssl=true', function(err, client, done) {
    if (err) {
        console.error(err);
        process.exit(1);
    }
    

    console.log('Querying tags');
    client.query('SELECT tag '+
                 'FROM public.hashtag ', function(err, result) {
                     if (err) { 
                         console.error(err);
                     } else {
                         var hashtags = result.rows;
                         var query = '';
                         result.rows.forEach(function(row){
                             query += ((query === '') ? '' : ',')+row.tag;
                         });
                         console.log('query: ', query);
                         console.log('hashtags: ', hashtags);

                         tw.stream('statuses/filter', {track: query}, function(stream) {
                             console.log('In tw stream: ', stream);
                             
                             stream.on('data', function(tweet) {
                                 console.log('Tweet: ',tweet);

                                 hashtags.forEach(function(hashtag){
                                     if (tweet.text.toLowerCase().indexOf(hashtag.tag.toLowerCase()) !== -1) {

                                         console.log('Inserting: ', tweet.user.screen_name, hashtag.tag, tweet.text);
                                         
                                         var insert = 'INSERT INTO public.tweet(screen_name, hashtag, text) '+
                                             'VALUES($1, $2, $3)';
                                         
                                         client.query(insert, [tweet.user.screen_name, hashtag.tag, tweet.text], function(err, result) {
                                             if(err) {
                                                 console.error(err);
                                             } else {
                                                 console.log('Inserted: ', tweet.id_str);
                                                 client.query('SELECT tag '+
                                                              'FROM public.hashtag ', function(err, result) {
                                                                  hashtags = result.rows;
                                                              });
                                         });
                                     }
                                 });

                             });
                             
                             stream.on('error', function(error) {
                                 console.error(error);
                                 throw error;
                             });
                         }); 
                     }
                 });
});
