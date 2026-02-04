# Read Replica

## Objetivo
Permitir leitura em replica sem impactar escrita no banco primario.

## Variavel
- `DATABASE_READ_URL` (opcional)

Se definida, endpoints de leitura usam essa conexao. Escritas sempre vao para `DATABASE_URL`.

## Limitacoes
- Lag de replica pode causar leitura atrasada.
- Use replica apenas para endpoints GET.

## Quando usar
- Dashboards
- Consultas pesadas
- Listagens grandes
