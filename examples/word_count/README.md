# Word count

Here, we will do a simple workflow that downloads several HTML files, extracts text from each HTML file, and then counts word frequencies.

This example uses BeautifulSoup for the text extraction, so you will need to install the dependencies using a command like:

```sh
pip install -r requirements.txt
```

Next, you can run the workflow end-to-end by using the following command:

```sh
redun run workflow.py main
```

That should produce output looking something like the following:

```
[redun] redun :: version 0.7.4
[redun] config dir: /Users/rasmus/projects/redun/examples/word_count/.redun
[redun] Start Execution 34b28d3d-153a-44cd-8076-ba0a98fe957a:  redun run workflow.py main
[redun] Run    Job c9202a5a:  redun.examples.word_count.main(urls_file=File(path=urls.txt, hash=f5adc73c), data_path='data') on default
[redun] Run    Job 5681a70b:  redun.examples.word_count.download(urls=['https://www.python.org', 'https://www.python.org/downloads', 'https://www.python.org/community', 'https://www.python.org/success-stories', 'https://www.python.org/events'], data_path='data') on default
[redun] Run    Job fe2ea7c9:  redun.examples.word_count.download_file(url='https://www.python.org', data_path='data') on default
[redun] Run    Job a32085b5:  redun.examples.word_count.download_file(url='https://www.python.org/downloads', data_path='data') on default
[redun] Run    Job 7285dcfb:  redun.examples.word_count.download_file(url='https://www.python.org/community', data_path='data') on default
[redun] Run    Job e15c299e:  redun.examples.word_count.download_file(url='https://www.python.org/success-stories', data_path='data') on default
[redun] Run    Job d81d865b:  redun.examples.word_count.download_file(url='https://www.python.org/events', data_path='data') on default
[redun] Run    Job 31cc0394:  redun.examples.word_count.extract_texts(html_files=[File(path=data/https//www.python.org/index.html, hash=700cf1b0), File(path=data/https//www.python.org/downloads/index.html, hash=571149f2), File(path=data/https//www.python.org/community/index.htm...) on default
[redun] Run    Job 5fa12bee:  redun.examples.word_count.extract_text(html_file=File(path=data/https//www.python.org/index.html, hash=700cf1b0)) on default
[redun] Run    Job 57632a58:  redun.examples.word_count.extract_text(html_file=File(path=data/https//www.python.org/downloads/index.html, hash=571149f2)) on default
[redun] Run    Job 21b7c3a2:  redun.examples.word_count.extract_text(html_file=File(path=data/https//www.python.org/community/index.html, hash=08a0522a)) on default
[redun] Run    Job 5fe4672f:  redun.examples.word_count.extract_text(html_file=File(path=data/https//www.python.org/success-stories/index.html, hash=4e3c0b0f)) on default
[redun] Run    Job 2b21200f:  redun.examples.word_count.extract_text(html_file=File(path=data/https//www.python.org/events/index.html, hash=fbb12ce2)) on default
[redun] Run    Job c969c9c1:  redun.examples.word_count.count_words(text_files=[File(path=data/https//www.python.org/index.txt, hash=1e3b3b92), File(path=data/https//www.python.org/downloads/index.txt, hash=2375c7d7), File(path=data/https//www.python.org/community/index.txt, ..., data_path='data') on default
[redun]
[redun] | JOB STATUS 2021/10/06 09:11:55
[redun] | TASK                                    PENDING RUNNING  FAILED  CACHED    DONE   TOTAL
[redun] |
[redun] | ALL                                           0       0       0       0      14      14
[redun] | redun.examples.word_count.count_words         0       0       0       0       1       1
[redun] | redun.examples.word_count.download            0       0       0       0       1       1
[redun] | redun.examples.word_count.download_file       0       0       0       0       5       5
[redun] | redun.examples.word_count.extract_text        0       0       0       0       5       5
[redun] | redun.examples.word_count.extract_texts       0       0       0       0       1       1
[redun] | redun.examples.word_count.main                0       0       0       0       1       1
[redun]
[redun] Execution duration: 1.28 seconds
File(path=data/word_counts.tsv, hash=ff779558)
```

After the workflow completes, we should have a tab-separated file of word frequencies in `data/word_counts.tsv`.

```
head data/word_counts.tsv

word	count
Python	367
Release	164
Download	163
Notes	152
the	82
and	71
to	66
for	59
Events	54
```

Feel free to add additional URLs to `urls.txt` and rerun the workflow. You should be able to see reactivity and only minimal part of the workflow performing incremental compute.

```sh
echo "https://www.python.org/blogs" >> urls.txt
redun run workflow.py main
```

Which should produce an output similar to:

```
[redun] redun :: version 0.7.4
[redun] config dir: /Users/rasmus/projects/redun/examples/word_count/.redun
[redun] Start Execution 2db59ec6-5a13-4a51-ad21-809d24ffe88b:  redun run workflow.py main
[redun] Run    Job ba39ea20:  redun.examples.word_count.main(urls_file=File(path=urls.txt, hash=06ff4ba1), data_path='data') on default
[redun] Run    Job bbf38b2e:  redun.examples.word_count.download(urls=['https://www.python.org', 'https://www.python.org/downloads', 'https://www.python.org/community', 'https://www.python.org/success-stories', 'https://www.python.org/events', 'https://www.python.org..., data_path='data') on default
[redun] Cached Job 625dc5c4:  redun.examples.word_count.download_file(url='https://www.python.org', data_path='data') (eval_hash=ec36a775)
[redun] Cached Job 2e898ead:  redun.examples.word_count.download_file(url='https://www.python.org/downloads', data_path='data') (eval_hash=2f26d757)
[redun] Cached Job 247ae2e4:  redun.examples.word_count.download_file(url='https://www.python.org/community', data_path='data') (eval_hash=b5b3cdbb)
[redun] Cached Job a0ef8251:  redun.examples.word_count.download_file(url='https://www.python.org/success-stories', data_path='data') (eval_hash=1112ad27)
[redun] Cached Job d9ac501e:  redun.examples.word_count.download_file(url='https://www.python.org/events', data_path='data') (eval_hash=696daf1b)
[redun] Run    Job 0b0f9183:  redun.examples.word_count.download_file(url='https://www.python.org/blogs', data_path='data') on default
[redun] Run    Job f3d33f8a:  redun.examples.word_count.extract_texts(html_files=[File(path=data/https//www.python.org/index.html, hash=5210f1c6), File(path=data/https//www.python.org/downloads/index.html, hash=83825454), File(path=data/https//www.python.org/community/index.htm...) on default
[redun] Cached Job 54d51ae2:  redun.examples.word_count.extract_text(html_file=File(path=data/https//www.python.org/index.html, hash=5210f1c6)) (eval_hash=0a2ad008)
[redun] Cached Job 645a2099:  redun.examples.word_count.extract_text(html_file=File(path=data/https//www.python.org/downloads/index.html, hash=83825454)) (eval_hash=6f54589e)
[redun] Cached Job 742e12b0:  redun.examples.word_count.extract_text(html_file=File(path=data/https//www.python.org/community/index.html, hash=8aaba28b)) (eval_hash=e3459733)
[redun] Cached Job 0838aab9:  redun.examples.word_count.extract_text(html_file=File(path=data/https//www.python.org/success-stories/index.html, hash=6e3b8512)) (eval_hash=3bccbbaf)
[redun] Cached Job 302c4f23:  redun.examples.word_count.extract_text(html_file=File(path=data/https//www.python.org/events/index.html, hash=88470204)) (eval_hash=433227b9)
[redun] Run    Job 21095dc1:  redun.examples.word_count.extract_text(html_file=File(path=data/https//www.python.org/blogs/index.html, hash=65944bf9)) on default
[redun] Run    Job 3b1f3ca2:  redun.examples.word_count.count_words(text_files=[File(path=data/https//www.python.org/index.txt, hash=3a76ad57), File(path=data/https//www.python.org/downloads/index.txt, hash=aefc842a), File(path=data/https//www.python.org/community/index.txt, ..., data_path='data') on default
[redun]
[redun] | JOB STATUS 2021/10/07 05:17:09
[redun] | TASK                                    PENDING RUNNING  FAILED  CACHED    DONE   TOTAL
[redun] |
[redun] | ALL                                           0       0       0      10       6      16
[redun] | redun.examples.word_count.count_words         0       0       0       0       1       1
[redun] | redun.examples.word_count.download            0       0       0       0       1       1
[redun] | redun.examples.word_count.download_file       0       0       0       5       1       6
[redun] | redun.examples.word_count.extract_text        0       0       0       5       1       6
[redun] | redun.examples.word_count.extract_texts       0       0       0       0       1       1
[redun] | redun.examples.word_count.main                0       0       0       0       1       1
[redun]
[redun] Execution duration: 0.99 seconds
File(path=data/word_counts.tsv, hash=2d67a3e2)
```

Notice, how most jobs are `Cached`, except the ones related to the URL `https://www.python.org/blogs/`.

We can inspect the job tree of the execution `34b28d3d` (or `-` for most recent execution) by using:

```sh
redun log -
```

```
Exec 34b28d3d-153a-44cd-8076-ba0a98fe957a [ DONE ] 2021-10-06 09:11:53:  run workflow.py main (git_commit=a6f63c50f783996632cde0791bbd6546ca324ac4, git_origin_url=git@github.com:insitro/redun.git, project=redun.examples.word_count, redun.version=0.7.4, user=rasmus)
Duration: 0:00:01.26

Jobs: 14 (DONE: 14, CACHED: 0, FAILED: 0)
--------------------------------------------------------------------------------
Job c9202a5a [ DONE ] 2021-10-06 09:11:53:  redun.examples.word_count.main(data_path='data', urls_file=File(path=urls.txt, hash=f5adc73c))
  Job 5681a70b [ DONE ] 2021-10-06 09:11:53:  redun.examples.word_count.download(['https://www.python.org', 'https://www.python.org/downloads', 'https://www.python.org/community', 'https://www.python.org/success-stories', 'https://www.python.org/events'], 'data')
    Job fe2ea7c9 [ DONE ] 2021-10-06 09:11:53:  redun.examples.word_count.download_file('https://www.python.org', 'data')
    Job a32085b5 [ DONE ] 2021-10-06 09:11:53:  redun.examples.word_count.download_file('https://www.python.org/downloads', 'data')
    Job 7285dcfb [ DONE ] 2021-10-06 09:11:53:  redun.examples.word_count.download_file('https://www.python.org/community', 'data')
    Job e15c299e [ DONE ] 2021-10-06 09:11:53:  redun.examples.word_count.download_file('https://www.python.org/success-stories', 'data')
    Job d81d865b [ DONE ] 2021-10-06 09:11:53:  redun.examples.word_count.download_file('https://www.python.org/events', 'data')
  Job 31cc0394 [ DONE ] 2021-10-06 09:11:54:  redun.examples.word_count.extract_texts([File(path=data/https//www.python.org/index.html, hash=700cf1b0), File(path=data/https//www.python.org/downloads/index.html, hash=571149f2), File(path=data/https//www.python.org/community/index.htm...)
    Job 5fa12bee [ DONE ] 2021-10-06 09:11:54:  redun.examples.word_count.extract_text(File(path=data/https//www.python.org/index.html, hash=700cf1b0))
    Job 57632a58 [ DONE ] 2021-10-06 09:11:54:  redun.examples.word_count.extract_text(File(path=data/https//www.python.org/downloads/index.html, hash=571149f2))
    Job 21b7c3a2 [ DONE ] 2021-10-06 09:11:54:  redun.examples.word_count.extract_text(File(path=data/https//www.python.org/community/index.html, hash=08a0522a))
    Job 5fe4672f [ DONE ] 2021-10-06 09:11:54:  redun.examples.word_count.extract_text(File(path=data/https//www.python.org/success-stories/index.html, hash=4e3c0b0f))
    Job 2b21200f [ DONE ] 2021-10-06 09:11:54:  redun.examples.word_count.extract_text(File(path=data/https//www.python.org/events/index.html, hash=fbb12ce2))
  Job c969c9c1 [ DONE ] 2021-10-06 09:11:54:  redun.examples.word_count.count_words([File(path=data/https//www.python.org/index.txt, hash=1e3b3b92), File(path=data/https//www.python.org/downloads/index.txt, hash=2375c7d7), File(path=data/https//www.python.org/community/index.txt, ..., 'data')
```

And we look at the derivation (i.e. data lineage) of our output file using this following command:

```
redun log data/word_counts.txt
```

Which should produce output similar to:

```
File ff779558 data/word_counts.tsv
Produced by Job c969c9c1

  Job c969c9c1-d563-4117-a587-4c6a44b2c977 [ DONE ] 2021-10-06 09:11:54:  redun.examples.word_count.count_words([File(path=data/https//www.python.org/index.txt, hash=1e3b3b92), File(path=data/https//www.python.org/downloads/index.txt, hash=2375c7d7), File(path=data/https//www.python.org/community/index.txt, ..., 'data')
  Traceback: Exec 34b28d3d > Job c9202a5a main > Job c969c9c1 count_words
  Duration: 0:00:00.04

    CallNode a927597b0e3c45194e0dda2925628b9043ee357e redun.examples.word_count.count_words
      Args:   [File(path=data/https//www.python.org/index.txt, hash=1e3b3b92), File(path=data/https//www.python.org/downloads/index.txt, hash=2375c7d7), File(path=data/https//www.python.org/community/index.txt, ..., 'data'
      Result: File(path=data/word_counts.tsv, hash=ff779558)

    Task 9d1ee8a5c0be9d0cc670a8f85fe2c5e8c6da5071 redun.examples.word_count.count_words

      def count_words(text_files: List[File], data_path: str) -> File:
          """
          Count the word frequencies in a list of text files.
          """
          counts = defaultdict(int)

          for text_file in text_files:
              words = text_file.read().strip().split()
              for word in words:
                  counts[word] += 1

          counts = sorted(counts.items(), key=lambda word_count: word_count[1], reverse=True)

          file = File(f"{data_path}/word_counts.tsv")
          with file.open("w") as out:
              writer = csv.writer(out, delimiter="\t")
              writer.writerow(["word", "count"])
              for row in counts:
                  writer.writerow(row)
          return file


    Upstream dataflow:

      result = File(path=data/word_counts.tsv, hash=ff779558)

      result <-- <a927597b> count_words(text_files, data_path)
        text_files = <c76fbce6> [File(path=data/https//www.python.org/index.txt, hash=1e3b3b92), File(path=data/https//www.python.org/downloads/index.txt, hash=2375c7d7), File(path=data/https//www.python.org/community/index.txt, ...
        data_path  = <fbb2a8bf> 'data'

      text_files <-- <3ce8cb2a> extract_texts(html_files)
        html_files = <8b3da236> [File(path=data/https//www.python.org/index.html, hash=700cf1b0), File(path=data/https//www.python.org/downloads/index.html, hash=571149f2), File(path=data/https//www.python.org/community/index.htm...

      html_files <-- <8e31f2b4> download(urls, data_path_2)
        urls        = <dc965bd1> ['https://www.python.org', 'https://www.python.org/downloads', 'https://www.python.org/community', 'https://www.pytho
n.org/success-stories', 'https://www.python.org/events']
        data_path_2 = <fbb2a8bf> 'data'

      data_path_2 <-- <d0192491> data_path_3

      data_path <-- <d0192491> data_path_3

      data_path_3 <-- origin      
```

