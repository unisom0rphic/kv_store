# PLAN
1. TESTS -> we need unit tests cause it's aura
2. fix arc with parser (if multiple tcp connections)
3. check multithreading scenarios (should data race but let's hope it won't)
4. review parser.parse() - seems not idiomatic enough
5. http server with `/metrics` ig