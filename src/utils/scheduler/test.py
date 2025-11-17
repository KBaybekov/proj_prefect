def _get_time_str(
                  
                      seconds:float = 0.0
                     ) -> str:
        """
        Конвертирует секунды в строку формата "HH:MM:SS".
        """
        if seconds:
            if seconds < 0:
                return "00:00:00"
            else:
                hours, remainder = divmod(int(seconds), 3600)
                minutes, seconds = divmod(remainder, 60)
                return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
        return "00:00:00"

print(_get_time_str(36009.45645))