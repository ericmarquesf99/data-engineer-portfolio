# 🧪 Teste Local do Notebook (01_extraction.py)

Como testar a célula do notebook localmente sem precisar do Databricks.

## Opção 1: Teste Rápido (Recomendado)

```python
# Abra um terminal Python e execute:
from datetime import datetime
import json
from src.extractors.coingecko_extractor import CryptoExtractor
from src.utils.logging_config import StructuredLogger
from src.utils.config_loader import load_config

# Configuração
run_id = f"test_run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
output_path = "logs/"  # Salvar em logs/ ao invés de dbfs:/
logger = StructuredLogger("extraction")

start_time = datetime.now()

try:
    # ✅ CORRIGIDO: Carregar config
    config = load_config()
    
    # ✅ CORRIGIDO: Passar config para o extrator
    extractor = CryptoExtractor(config)
    
    # Obter dados de múltiplas criptomoedas
    crypto_ids = [
        'bitcoin', 'ethereum', 'binancecoin', 'cardano', 'solana',
        'polkadot', 'dogecoin', 'avalanche-2', 'polygon', 'chainlink'
    ]
    
    logger.log_event("fetching_crypto_data", {"coins": len(crypto_ids)})
    
    all_data = []
    for crypto_id in crypto_ids:
        data = extractor.get_crypto_data(crypto_id)
        if data:
            all_data.append(data)
            print(f"  ✅ {crypto_id}")
    
    # Adicionar metadados
    extraction_metadata = {
        "extraction_timestamp": datetime.now().isoformat(),
        "run_id": run_id,
        "source": "coingecko_api_v3",
        "record_count": len(all_data),
        "crypto_ids": crypto_ids
    }
    
    # Salvar em arquivo local (não DBFS)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"{output_path}crypto_data_{timestamp}.json"
    
    full_data = {
        "metadata": extraction_metadata,
        "data": all_data
    }
    
    # Salvar no arquivo local
    import os
    os.makedirs(output_path, exist_ok=True)
    
    with open(output_file, 'w') as f:
        json.dump(full_data, f, indent=2)
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    logger.log_event("extraction_completed", {
        "file": output_file,
        "records": len(all_data),
        "duration_seconds": duration
    })
    
    # Resultado
    result = {
        "status": "success",
        "output_file": output_file,
        "record_count": len(all_data),
        "duration_seconds": duration
    }
    
    print(f"\n✅ Extração completa: {len(all_data)} criptomoedas")
    print(f"📁 Arquivo salvo: {output_file}")
    print(f"⏱️  Duração: {duration:.2f}s")
    print(json.dumps(result, indent=2))
    
except Exception as e:
    logger.log_event("extraction_error", {"error": str(e)}, level="ERROR")
    print(f"❌ ERRO: {e}")
    import traceback
    traceback.print_exc()
```

**Resultado esperado:**
```
  ✅ bitcoin
  ✅ ethereum
  ✅ binancecoin
  ...
✅ Extração completa: 10 criptomoedas
📁 Arquivo salvo: logs/crypto_data_20260119_143000.json
⏱️  Duração: 15.34s
{
  "status": "success",
  "record_count": 10,
  "duration_seconds": 15.34
}
```

---

## Opção 2: Teste via Script Salvo

Crie arquivo `test_extraction_local.py`:

```python
#!/usr/bin/env python
"""
Teste local do notebook 01_extraction
"""
import sys
sys.path.insert(0, 'src')

from datetime import datetime
import json
import os
from extractors.coingecko_extractor import CryptoExtractor
from utils.logging_config import StructuredLogger
from utils.config_loader import load_config

def main():
    run_id = f"test_run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    output_path = "logs/"
    logger = StructuredLogger("extraction")
    
    start_time = datetime.now()
    
    try:
        # Carregar configuração
        config = load_config()
        
        # Criar extrator
        extractor = CryptoExtractor(config)
        
        # Criptomoedas a extrair
        crypto_ids = [
            'bitcoin', 'ethereum', 'binancecoin', 'cardano', 'solana',
            'polkadot', 'dogecoin', 'avalanche-2', 'polygon', 'chainlink'
        ]
        
        logger.log_event("fetching_crypto_data", {"coins": len(crypto_ids)})
        print(f"🔄 Extraindo {len(crypto_ids)} criptomoedas...")
        
        all_data = []
        for crypto_id in crypto_ids:
            data = extractor.get_crypto_data(crypto_id)
            if data:
                all_data.append(data)
                price = data.get('current_price', 'N/A')
                print(f"  ✅ {crypto_id}: ${price}")
        
        # Metadados
        extraction_metadata = {
            "extraction_timestamp": datetime.now().isoformat(),
            "run_id": run_id,
            "source": "coingecko_api_v3",
            "record_count": len(all_data),
            "crypto_ids": crypto_ids
        }
        
        # Salvar arquivo
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        os.makedirs(output_path, exist_ok=True)
        output_file = f"{output_path}crypto_data_{timestamp}.json"
        
        full_data = {
            "metadata": extraction_metadata,
            "data": all_data
        }
        
        with open(output_file, 'w') as f:
            json.dump(full_data, f, indent=2)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logger.log_event("extraction_completed", {
            "file": output_file,
            "records": len(all_data),
            "duration_seconds": duration
        })
        
        # Resultado
        print(f"\n✅ SUCESSO!")
        print(f"   Arquivo: {output_file}")
        print(f"   Registros: {len(all_data)}")
        print(f"   Duração: {duration:.2f}s")
        
        return {
            "status": "success",
            "file": output_file,
            "records": len(all_data),
            "duration": duration
        }
        
    except Exception as e:
        logger.log_error("extraction_failed", e)
        print(f"\n❌ FALHA: {e}")
        import traceback
        traceback.print_exc()
        return {"status": "failed", "error": str(e)}

if __name__ == "__main__":
    result = main()
    sys.exit(0 if result["status"] == "success" else 1)
```

Execute com:
```bash
python test_extraction_local.py
```

---

## Opção 3: Teste Passo-a-Passo (Debug)

```bash
# Terminal
python << 'EOF'
# Passo 1: Importar
print("1️⃣ Importando...")
from src.utils.config_loader import load_config
print("   ✅ Config loader OK")

from src.extractors.coingecko_extractor import CryptoExtractor
print("   ✅ CryptoExtractor OK")

# Passo 2: Carregar config
print("2️⃣ Carregando config...")
config = load_config()
print(f"   ✅ Config carregada")
print(f"   API timeout: {config['api']['coingecko']['timeout']}s")

# Passo 3: Criar extrator
print("3️⃣ Criando extrator...")
extractor = CryptoExtractor(config)
print("   ✅ Extrator criado")

# Passo 4: Extrair 1 crypto
print("4️⃣ Extraindo bitcoin...")
data = extractor.get_crypto_data('bitcoin')
if data:
    print(f"   ✅ Preço: ${data.get('current_price')}")
else:
    print("   ❌ Nenhum dado retornado")

# Passo 5: Extrair múltiplas
print("5️⃣ Extraindo múltiplas...")
cryptos = ['bitcoin', 'ethereum', 'cardano']
results = []
for crypto in cryptos:
    data = extractor.get_crypto_data(crypto)
    if data:
        results.append(data)
        print(f"   ✅ {crypto}")

print(f"\n✅ TOTAL: {len(results)} criptos extraídos")
EOF
```

---

## ✅ Checklist

- [ ] `config/config.yaml` existe
- [ ] `.env` existe com credenciais
- [ ] `src/utils/config_loader.py` existe
- [ ] `src/extractors/coingecko_extractor.py` existe
- [ ] Teste passa sem erros

---

## Correções Feitas

✅ **Agora todos os notebooks têm:**
1. Import de `load_config`
2. Chamada de `load_config()` 
3. Passagem correta de `config` para os construtores

**Antes:**
```python
extractor = CryptoExtractor()  # ❌ Erro!
```

**Depois:**
```python
config = load_config()  # ✅ Carrega config
extractor = CryptoExtractor(config)  # ✅ OK!
```
