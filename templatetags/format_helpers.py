
from django import template

register = template.Library()


@register.filter
def format_timedelta(timedelta_obj):
    """
    Formata um objeto timedelta para o formato HH:MM:SS, ignorando milissegundos.
    """
    if not timedelta_obj:
        return ""

    # Pega o total de segundos, ignorando a parte fracionária
    total_seconds = int(timedelta_obj.total_seconds())

    # Calcula dias, horas, minutos e segundos
    days, remainder = divmod(total_seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, seconds = divmod(remainder, 60)

    if days > 0:
        return f'{days}d {hours:02}:{minutes:02}:{seconds:02}'
    else:
        return f'{hours:02}:{minutes:02}:{seconds:02}'