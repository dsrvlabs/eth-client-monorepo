# fixture — spaced spelling of a nonexistent id must still be extracted

Revision 1 missed `ADR P8-99`. The extractor must catch the space and
fail because the id resolves under neither the table nor an ADR file.
