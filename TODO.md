- Replace fmt.Errorf with errors.New() and compare results
- Replace calls like sql.WriteString(strings.Join(b.Options, " ")) with adding data in loop. Thus remove extra memory allocation. Don't forget benchmarks. Something like:
```
func (b *buffer) WriteStrings(s []string, separator string)
```