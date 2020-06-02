# Micro Map Reduce  

The Map Reduce framework was originally designed and implemented at Google to handle massive amounts of data processing for their search engine. The original Map Reduce framework abstracts away complex and  error prone code for Google scale parallel processing jobs by distributing Map and Reduce code across cheap commodity hardware. Using the constraints of Map and Reduce , simple code could be written that would otherwise be extraordinarily complex.

But what if the idea of using Map and Reduce jobs can solve more general coding problems that have nothing to do with large scale data processing? That is what the Micro Map Reduce framework attempts to solve.

Think about it: If parallel code is always difficult to write what if a framework existed that could be imported into your codebase that could abstract away all parallel programming code no matter the use case? 

**This framework does this by implementing a Map Reduce interface over multi threaded code rather than over commodity hardware.** This allows for any job, not just massive parallel processing jobs, to be written in terms of Map and Reduce to simplify a codebase. 

This improves code performance drastically by taking advantage of multi threaded programming, dramatically simplifies the codebase by abstracting away complex multi threaded programming for any kind of job, and overall speeds up a teams workflow by not having to waste time on writing and debugging complex multi threaded code. 

## Current State Of Project and Limitations

All of the core logic is completely implemented and heavily tested for multiple worker failures.

One further iteration is needed to package the Micro Map Reduce framework into a useful product. The roadblock is it currently uses text files as inputs, so a simple interface would need to be created so it can take in data structures instead. Also the codebase could use some organizing and cleaning. This is all simple and easy to implement.

**The major limitation** to this project in it's current state is that it was implemented in Golang and not C. Golang's paradigm makes it simple to implement multithreaded programming. That defeats the purpose of the framework. Also Golang makes it so it can't be easily imported into a different language, where the problem would be viable. This severely limits the Micro Map Reduce framework's practicality. 

For it to be worthwhile, **it needs to be re-written in C**, and then used in C, C++, and Python (via C). This would make the framework incredibly useful in easily making any complex multiprocessing code very simple in languages where it is very hard (C, C++) or not as practical (Python - no native multithreading).
