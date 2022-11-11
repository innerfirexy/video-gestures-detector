## Scroll a YouTube channel page:
```
var scroll = setInterval(function(){window.scrollBy(0,1000)}, 1000);
```

## List all videos as a table (not working 2022.11.10)
```
window.clearInterval(scroll); console.clear(); urls = $$('a'); urls.forEach(function(v,i,a) {if (v.id=="video-title") {console.log('\t'+v.title+'\t'+v.href+'\t')}});
```
