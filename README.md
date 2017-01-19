# spark-data-exploration
This programe is to perform some data exploration of page view statistics for Wikimedia
projects.  

For input, you can download any Pagecount files for 2016-08. These files can be found at
https://dumps.wikimedia.org/other/pagecounts-raw/2016/2016-08/ with .gz extension. For
example, one such file is https://dumps.wikimedia.org/other/pagecounts-raw/2016/2016-
08/pagecounts-20160801-040000.gz


The schema is <Language code> <Page title> <View count> <Page size>. Each of these fields is
delimited by space. More about the format of these files can be found at
https://dumps.wikimedia.org/other/pagecounts-raw/ 

The first field (Language code) can have some text after a dot, for example fr.b. We want
to only get the language code and ignore the text after the dot. So instead of fr.b, we must use fr for
example. This can be done by using the org.apache.commons.lang3.StringUtils library. To get the
text before the dot, we can simply call it as StringUtils.substringBefore(text, ".").
Also, you must filter out records, where the Page title is the same as the Language code. For
example, if you have a record where the Language code is fr and the Page title is also fr, filter it out.
The task of this assignment is to output information in the following form.
<Language>,<Language-code>,<TotalViewsInThatLang>,<MostVisitedPageInThatLang>,<ViewsOfThatPage>
So each line has a record for a different language, where you have the name of that language, its
code, the combined views of all pages in that language, the title of the most visited page in that
language, and also the views of that most visited page in that language. 

The records should be sorted such that the language with the most views overall (Views of all pages
in that language combined) should be listed first. In other words, records are listed in descending
order according to the total number of views of pages in different languages. 
