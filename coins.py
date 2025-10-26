import psycopg2
import sys

# Твой connection string
CONNECTION_STRING = "postgresql://coin_sifter_db_43u4_user:o2JLzcpHrsZC7lBBOQDpDVLhWQxTL1H7@dpg-d3jodet6ubrc73d0h4dg-a.frankfurt-postgres.render.com/coin_sifter_db_43u4"

# Имя таблицы, найденное в database.py
TABLE_TO_CHECK = "monthly_coin_selection" 


def check_coin_count():
    """
    Подключается к PostgreSQL и проверяет количество записей в таблице.
    """
    conn = None
    try:
        # 1. Подключаемся к базе данных
        print(f"Попытка подключения к {CONNECTION_STRING.split('@')[-1]}...")
        conn = psycopg2.connect(CONNECTION_STRING)
        print("✅ Успешное подключение!")
        
        # 2. Создаем курсор
        with conn.cursor() as cursor:
            
            # 3. Выполняем запрос
            query = f"SELECT COUNT(*) FROM {TABLE_TO_CHECK};"
            print(f"Выполняю запрос: {query}")
            
            cursor.execute(query)
            
            # 4. Получаем результат
            result = cursor.fetchone()
            
            if result:
                count = result[0]
                print("\n" + "="*30)
                print(f"РЕЗУЛЬТАТ: Найдено {count} записей в таблице '{TABLE_TO_CHECK}'.")
                print("="*30 + "\n")
                
                if count == 0:
                    print("⚠️  ВНИМАНИЕ: Таблица пуста. Это может быть причиной ошибки [CRON_JOB] на сервере.")
                else:
                    print("👍 Это хорошо. Проблема [CRON_JOB] может быть в другом месте.")
            else:
                print("❌ Не удалось получить результат подсчета.")

    except psycopg2.OperationalError as e:
        print(f"💥 ОШИКА ПОДКЛЮЧЕНИЯ: Не удалось подключиться к базе данных.", file=sys.stderr)
        print(f"   Подробности: {e}", file=sys.stderr)
        print("   Проверь IP-адреса в 'allow list' на Render.com и правильность connection string.", file=sys.stderr)
        
    except psycopg2.Error as e:
        # Проверяем на ошибку "relation does not exist" (таблица не найдена)
        if "relation" in str(e) and "does not exist" in str(e):
            print(f"💥 ОШИБКА ЗАПРОСА: Таблица с именем '{TABLE_TO_CHECK}' не найдена!", file=sys.stderr)
            print("   Пожалуйста, исправь переменную TABLE_TO_CHECK в этом скрипте.", file=sys.stderr)
        else:
            print(f"💥 ОБЩАЯ ОШИБКА psycopg2: {e}", file=sys.stderr)
            
    except Exception as e:
        print(f"💥 НЕИЗВЕСТНАЯ ОШИБКА: {e}", file=sys.stderr)

    finally:
        # 5. Закрываем соединение
        if conn:
            conn.close()
            print("Соединение с БД закрыто.")

if __name__ == "__main__":
    check_coin_count()

