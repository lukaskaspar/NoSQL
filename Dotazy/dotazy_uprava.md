# Dotazy

Dotazy v tomto dokumentu jsou navrženy pro spuštění v prostředí `mongosh` nad Yelp datasetem. Pracují s kolekcemi `businesses`, `reviews` a `users` a využívají zejména agregační pipeline, operátor `$lookup`, práci s poli a kontrolní dotazy pro ověření kvality dat.

U většiny agregačních dotazů je použito nastavení `{ allowDiskUse: true }`, aby bylo možné bezpečně zpracovat i objemnější mezivýsledky při běhu nad většími kolekcemi.

## Inicializační indexy

Před spuštěním analytických dotazů jsou vytvořeny základní sekundární indexy nad často filtrovanými a propojovacími poli. Tyto indexy snižují náklady na vyhledávání a zlepšují výkon dotazů využívajících filtrování i běhové propojení kolekcí.

    db.reviews.createIndex({ business_id: 1 })
    db.reviews.createIndex({ user_id: 1 })
    db.reviews.createIndex({ date: 1 })
    db.reviews.createIndex({ stars: 1 })

    db.businesses.createIndex({ business_id: 1 })
    db.businesses.createIndex({ city: 1 })
    db.businesses.createIndex({ state: 1 })
    db.businesses.createIndex({ stars: 1 })
    db.businesses.createIndex({ is_open: 1 })

    db.users.createIndex({ user_id: 1 })


## 1. Agregační a analytické dotazy

Dotazy v této části využívají agregační pipeline nad kolekcemi `businesses`, `reviews` a `users`. Zaměřují se na distribuční přehledy, časové trendy, odvozené metriky a základní analytické souhrny nad provozními daty Yelp datasetu.

### Dotaz 1: Počet podniků v jednotlivých státech
Agregační pipeline seskupí dokumenty z kolekce `businesses` podle pole `state` a pro každou skupinu vypočítá kardinalitu pomocí akumulátoru `$sum`. Úvodní `$project` omezuje zpracovávané schéma pouze na potřebný atribut, čímž snižuje objem dat vstupujících do fáze `$group`. Výstup je následně seřazen sestupně podle počtu podniků, takže dotaz poskytuje přehled geografické distribuce záznamů napříč státy.

    db.businesses.aggregate(
      [
        {
          $project: {
            state: 1
          }
        },
        {
          $group: {
            _id: "$state",
            business_count: { $sum: 1 }
          }
        },
        {
          $sort: { business_count: -1 }
        }
      ],
      { allowDiskUse: true }
    )

### Dotaz 2: Průměrné hodnocení podniků podle města
Pipeline agreguje podniky na úrovni měst a současně počítá dvě metriky: průměrné hodnocení (`$avg`) a velikost vzorku (`$sum`). Fáze `$match` aplikovaná až po `$group` odstraňuje města s nízkou četností záznamů, čímž omezuje statistický šum a zvyšuje interpretovatelnost výsledku. Dotaz je vhodný pro porovnání lokalit podle kvality hodnocení při zachování minimální reprezentativnosti dat.

    db.businesses.aggregate(
      [
        {
          $project: {
            city: 1,
            stars: 1
          }
        },
        {
          $group: {
            _id: "$city",
            avg_stars: { $avg: "$stars" },
            business_count: { $sum: 1 }
          }
        },
        {
          $match: {
            business_count: { $gte: 20 }
          }
        },
        {
          $sort: { avg_stars: -1 }
        }
      ],
      { allowDiskUse: true }
    )

### Dotaz 3: Počet recenzí podle roku
Dotaz převádí textové datum recenze na časovou dimenzi roku pomocí kombinace `$toDate` a `$year`, následně vytváří roční agregaci počtu recenzí. Výsledkem je časová řada aktivity uživatelů nad kolekcí `reviews`, která umožňuje sledovat vývoj objemu recenzní činnosti v čase. Seřazení podle `_id` vrací chronologicky uspořádaný výstup vhodný pro další analytické zpracování nebo vizualizaci.

    db.reviews.aggregate(
      [
        {
          $project: {
            year: { $year: { $toDate: "$date" } }
          }
        },
        {
          $group: {
            _id: "$year",
            review_count: { $sum: 1 }
          }
        },
        {
          $sort: { _id: 1 }
        }
      ],
      { allowDiskUse: true }
    )

### Dotaz 4: Průměrná hodnota useful podle roku
Agregační pipeline odvozuje z data recenze kalendářní rok a pro každý rok počítá průměrnou hodnotu pole `useful` společně s počtem záznamů. Dotaz tak nekvantifikuje pouze objem aktivity, ale i průměrnou míru užitečnosti recenzí v jednotlivých obdobích. Výsledek lze využít pro analýzu vývoje engagement metrik a změn v kvalitě nebo percepci uživatelského obsahu.

    db.reviews.aggregate(
      [
        {
          $project: {
            year: { $year: { $toDate: "$date" } },
            useful: 1
          }
        },
        {
          $group: {
            _id: "$year",
            avg_useful: { $avg: "$useful" },
            review_count: { $sum: 1 }
          }
        },
        {
          $sort: { _id: 1 }
        }
      ],
      { allowDiskUse: true }
    )

### Dotaz 5: Uživatelé s nejvyšším součtem reakcí
Dotaz provádí per‑user agregaci reakcí uložených u recenzí a sčítá samostatně metriky `useful`, `funny` a `cool`. V následné projekci jsou tyto tři dimenze sloučeny do jedné souhrnné metriky `total_reactions`, která usnadňuje ranking uživatelů podle celkového dopadu jejich obsahu. Pipeline současně zachovává i počet recenzí, takže je možné porovnávat absolutní interakce s produkční aktivitou jednotlivých autorů.

    db.reviews.aggregate(
      [
        {
          $project: {
            user_id: 1,
            useful: 1,
            funny: 1,
            cool: 1
          }
        },
        {
          $group: {
            _id: "$user_id",
            total_useful: { $sum: "$useful" },
            total_funny: { $sum: "$funny" },
            total_cool: { $sum: "$cool" },
            total_reviews: { $sum: 1 }
          }
        },
        {
          $project: {
            total_reviews: 1,
            total_reactions: {
              $add: ["$total_useful", "$total_funny", "$total_cool"]
            }
          }
        },
        {
          $sort: { total_reactions: -1 }
        },
        {
          $limit: 10
        }
      ],
      { allowDiskUse: true }
    )

### Dotaz 6: Nejlépe hodnocené kategorie podniků
Pole `categories` je nejprve normalizováno do řádkové podoby pomocí `$unwind`, což umožní nad každou kategorií provést samostatnou agregaci. Pro každou kategorii se následně počítá průměrné hodnocení podniků a počet podniků v dané kategorii. Filtrem `business_count >= 30` jsou odstraněny málo zastoupené kategorie a výsledkem je žebříček kategorií s nejvyšším průměrným ratingem při minimální datové robustnosti.

    db.businesses.aggregate(
      [
        {
          $project: {
            categories: 1,
            stars: 1
          }
        },
        {
          $unwind: "$categories"
        },
        {
          $group: {
            _id: "$categories",
            avg_stars: { $avg: "$stars" },
            business_count: { $sum: 1 }
          }
        },
        {
          $match: {
            business_count: { $gte: 30 }
          }
        },
        {
          $sort: { avg_stars: -1 }
        }
      ],
      { allowDiskUse: true }
    )


## 2. Propojování kolekcí pomocí `$lookup`

Tato skupina ukazuje denormalizaci dat za běhu pomocí operátoru `$lookup`. Dotazy kombinují transakční data z kolekcí `reviews`, `users` a `businesses`, aby bylo možné vytvářet analytické výstupy přes více entit bez nutnosti trvalého předpočítání.

### Dotaz 7: Recenze se jménem uživatele a názvem podniku
Pipeline demonstruje běhové propojení tří kolekcí prostřednictvím dvojice operátorů `$lookup`, které připojují dimenzi uživatele a dimenzi podniku k jednotlivým recenzím. Operátory `$unwind` převádějí výsledné jednoprvkové joinované pole na plochou strukturu a závěrečný `$project` vytváří denormalizovaný výstup vhodný pro prezentaci. Dotaz je typickou ukázkou dokumentového joinu při zachování referenční vazby přes `user_id` a `business_id`.

    db.reviews.aggregate(
      [
        {
          $project: {
            review_id: 1,
            user_id: 1,
            business_id: 1,
            stars: 1,
            date: 1,
            text: 1
          }
        },
        {
          $lookup: {
            from: "users",
            localField: "user_id",
            foreignField: "user_id",
            as: "user"
          }
        },
        {
          $lookup: {
            from: "businesses",
            localField: "business_id",
            foreignField: "business_id",
            as: "business"
          }
        },
        {
          $unwind: "$user"
        },
        {
          $unwind: "$business"
        },
        {
          $project: {
            _id: 0,
            review_id: 1,
            stars: 1,
            date: 1,
            text: 1,
            user_name: "$user.name",
            business_name: "$business.name",
            city: "$business.city"
          }
        },
        {
          $limit: 20
        }
      ],
      { allowDiskUse: true }
    )

### Dotaz 8: Nejaktivnější uživatelé u podniků s hodnocením alespoň 4.5
Dotaz nejprve doplňuje ke každé recenzi metadata hodnoceného podniku a následně filtruje pouze recenze vztahující se k podnikům s ratingem alespoň `4.5`. Po redukci dat je provedena agregace na úrovni uživatele, která určuje, kdo nejčastěji recenzuje vysoce hodnocené podniky. Druhý `$lookup` pak enrichuje výsledek o jméno uživatele, takže výstup kombinuje analytickou metriku s identifikačními údaji entity.

    db.reviews.aggregate(
      [
        {
          $project: {
            user_id: 1,
            business_id: 1
          }
        },
        {
          $lookup: {
            from: "businesses",
            localField: "business_id",
            foreignField: "business_id",
            as: "business"
          }
        },
        {
          $unwind: "$business"
        },
        {
          $match: {
            "business.stars": { $gte: 4.5 }
          }
        },
        {
          $group: {
            _id: "$user_id",
            review_count_for_top_businesses: { $sum: 1 }
          }
        },
        {
          $lookup: {
            from: "users",
            localField: "_id",
            foreignField: "user_id",
            as: "user"
          }
        },
        {
          $unwind: "$user"
        },
        {
          $project: {
            _id: 0,
            user_id: "$_id",
            user_name: "$user.name",
            review_count_for_top_businesses: 1
          }
        },
        {
          $sort: { review_count_for_top_businesses: -1 }
        },
        {
          $limit: 10
        }
      ],
      { allowDiskUse: true }
    )

### Dotaz 9: Podniky hodnocené elitními uživateli
V první fázi jsou recenze obohaceny o profil autora z kolekce `users`, přičemž filtr ``user.elite.0: { $exists: true }`` identifikuje uživatele s neprázdným polem `elite`. Následná agregace seskupuje recenze podle `business_id` a určuje, které podniky získaly nejvíce hodnocení od elitních recenzentů. Závěrečný join s kolekcí `businesses` doplní obchodní název a lokalitu podniku pro snadnější interpretaci výsledků.

    db.reviews.aggregate(
      [
        {
          $project: {
            user_id: 1,
            business_id: 1
          }
        },
        {
          $lookup: {
            from: "users",
            localField: "user_id",
            foreignField: "user_id",
            as: "user"
          }
        },
        {
          $unwind: "$user"
        },
        {
          $match: {
            "user.elite.0": { $exists: true }
          }
        },
        {
          $group: {
            _id: "$business_id",
            elite_review_count: { $sum: 1 }
          }
        },
        {
          $lookup: {
            from: "businesses",
            localField: "_id",
            foreignField: "business_id",
            as: "business"
          }
        },
        {
          $unwind: "$business"
        },
        {
          $project: {
            _id: 0,
            business_id: "$_id",
            business_name: "$business.name",
            city: "$business.city",
            elite_review_count: 1
          }
        },
        {
          $sort: { elite_review_count: -1 }
        },
        {
          $limit: 15
        }
      ],
      { allowDiskUse: true }
    )

### Dotaz 10: uživatelé, kteří nejčastěji recenzují podniky v jednom dominantním státě
Tento dotaz vytváří dvoustupňovou agregaci nad kombinací uživatel–stát. Nejprve spočítá počty recenzí pro každou dvojici (`user_id`, `state`), následně po předřazení sestupně podle `review_count` použije druhý `$group`, ve kterém operátor `$first` vybere dominantní stát daného uživatele. Výstup tak nevrací pouze binární informaci o aktivitě ve státech, ale identifikuje primární geografické zaměření recenzní aktivity každého uživatele.

    db.reviews.aggregate(
      [
        {
          $project: {
            user_id: 1,
            business_id: 1
          }
        },
        {
          $lookup: {
            from: "businesses",
            localField: "business_id",
            foreignField: "business_id",
            as: "business"
          }
        },
        {
          $unwind: "$business"
        },
        {
          $match: {
            "business.state": { $exists: true, $ne: null, $ne: "" }
          }
        },
        {
          $group: {
            _id: {
              user_id: "$user_id",
              state: "$business.state"
            },
            review_count: { $sum: 1 }
          }
        },
        {
          $sort: {
            "_id.user_id": 1,
            review_count: -1
          }
        },
        {
          $group: {
            _id: "$_id.user_id",
            dominant_state: { $first: "$_id.state" },
            dominant_state_review_count: { $first: "$review_count" }
          }
        },
        {
          $match: {
            dominant_state_review_count: { $gte: 3 }
          }
        },
        {
          $sort: {
            dominant_state_review_count: -1
          }
        },
        {
          $limit: 20
        },
        {
          $lookup: {
            from: "users",
            localField: "_id",
            foreignField: "user_id",
            as: "user"
          }
        },
        {
          $unwind: "$user"
        },
        {
          $project: {
            _id: 0,
            user_id: "$_id",
            user_name: "$user.name",
            dominant_state: 1,
            dominant_state_review_count: 1
          }
        }
      ],
      { allowDiskUse: true }
    )

### Dotaz 11: Podniky, kde se skutečný průměr recenzí liší od business.stars
Pipeline nejprve počítá skutečný průměr hvězdiček přímo z kolekce `reviews` agregovaný podle `business_id`, čímž vzniká referenční hodnota odvozená z primárních dat. Po spojení s kolekcí `businesses` je vypočten rozdíl mezi uloženým agregátem `business.stars` a nově dopočteným `real_avg_stars`. Dotaz slouží jako kontrolní mechanismus konzistence denormalizovaných metrik a umožňuje odhalit nesoulad mezi předpočítaným a zdrojovým stavem.

    db.reviews.aggregate(
      [
        {
          $project: {
            business_id: 1,
            stars: 1
          }
        },
        {
          $group: {
            _id: "$business_id",
            real_avg_stars: { $avg: "$stars" },
            real_review_count: { $sum: 1 }
          }
        },
        {
          $match: {
            real_review_count: { $gte: 20 }
          }
        },
        {
          $lookup: {
            from: "businesses",
            localField: "_id",
            foreignField: "business_id",
            as: "business"
          }
        },
        {
          $unwind: "$business"
        },
        {
          $project: {
            _id: 0,
            business_id: "$_id",
            business_name: "$business.name",
            stored_stars: "$business.stars",
            real_avg_stars: 1,
            real_review_count: 1,
            diff: { $subtract: ["$real_avg_stars", "$business.stars"] }
          }
        },
        {
          $match: {
            diff: { $ne: 0 }
          }
        },
        {
          $sort: { diff: -1 }
        }
      ],
      { allowDiskUse: true }
    )

### Dotaz 12: nejlépe hodnocené podniky podle recenzí v jednotlivých městech
Dotaz kombinuje recenze s metadaty podniků, poté vytváří agregaci na úrovni dvojice město–podnik a počítá průměrné hodnocení z recenzí i počet recenzí. Po seřazení podle města, průměrného ratingu a objemu recenzí následuje druhá grupace, ve které je pomocí `$first` vybrán nejlepší podnik v rámci každého města. Jde o implementaci vzoru top‑1 per partition, kde je partition definována polem `city`.

    db.reviews.aggregate(
      [
        {
          $project: {
            business_id: 1,
            stars: 1
          }
        },
        {
          $lookup: {
            from: "businesses",
            localField: "business_id",
            foreignField: "business_id",
            as: "business"
          }
        },
        {
          $unwind: "$business"
        },
        {
          $match: {
            "business.city": { $exists: true, $ne: null, $ne: "" }
          }
        },
        {
          $group: {
            _id: {
              city: "$business.city",
              business_id: "$business_id"
            },
            business_name: { $first: "$business.name" },
            avg_review_stars: { $avg: "$stars" },
            review_count: { $sum: 1 }
          }
        },
        {
          $match: {
            review_count: { $gte: 5 }
          }
        },
        {
          $sort: {
            "_id.city": 1,
            avg_review_stars: -1,
            review_count: -1
          }
        },
        {
          $group: {
            _id: "$_id.city",
            best_business_id: { $first: "$_id.business_id" },
            best_business_name: { $first: "$business_name" },
            avg_review_stars: { $first: "$avg_review_stars" },
            review_count: { $first: "$review_count" }
          }
        },
        {
          $sort: {
            avg_review_stars: -1,
            review_count: -1
          }
        },
        {
          $limit: 20
        }
      ],
      { allowDiskUse: true }
    )


## 3. Práce s poli a vnořenými dokumenty

V této části jsou využity operátory určené pro práci s poli a nested strukturami, zejména `$unwind`, `$size`, `$ifNull` a `$objectToArray`. Dotazy ukazují, jak z dokumentově orientovaného modelu získat agregované metriky i nad vícenásobnými či vnořenými atributy.

### Dotaz 13: Počet podniků v jednotlivých kategoriích
Pole kategorií uložené u podniků je převedeno do normalizované podoby přes `$unwind`, aby každá kategorie mohla vstoupit do agregace jako samostatný řádek. Následná grupace vypočítá počet výskytů každé kategorie napříč kolekcí `businesses`. Výsledný seznam reprezentuje četnostní distribuci kategorií a současně ukazuje základní práci s vícenásobnými hodnotami v dokumentovém schématu.

    db.businesses.aggregate(
      [
        {
          $project: {
            categories: 1
          }
        },
        {
          $unwind: "$categories"
        },
        {
          $group: {
            _id: "$categories",
            business_count: { $sum: 1 }
          }
        },
        {
          $sort: { business_count: -1 }
        }
      ],
      { allowDiskUse: true }
    )

### Dotaz 14: Nejlépe hodnocené podniky v každé kategorii
Tato pipeline používá kombinaci `$unwind`, `$sort` a `$group` k výběru nejlepšího podniku v rámci každé kategorie. Seřazení podle názvu kategorie, počtu hvězdiček a `review_count` vytváří pořadí kandidátů, z něhož následný `$group` pomocí `$first` vyzvedne nejvýše umístěný dokument. Výsledkem je přehled nejlépe hodnocených reprezentantů jednotlivých kategorií bez nutnosti složitějšího okenního zpracování.

    db.businesses.aggregate(
      [
        {
          $project: {
            name: 1,
            categories: 1,
            stars: 1,
            review_count: 1
          }
        },
        {
          $unwind: "$categories"
        },
        {
          $sort: {
            categories: 1,
            stars: -1,
            review_count: -1
          }
        },
        {
          $group: {
            _id: "$categories",
            best_business_name: { $first: "$name" },
            best_stars: { $first: "$stars" },
            review_count: { $first: "$review_count" }
          }
        },
        {
          $sort: { best_stars: -1 }
        }
      ],
      { allowDiskUse: true }
    )

### Dotaz 15: Uživatelé s nejvyšším počtem přátel
Dotaz pracuje s polem `friends`, jehož velikost je odvozena operátorem `$size`; pro bezpečné zpracování případů bez vyplněného pole je použit wrapper `$ifNull`. Tím je zajištěno, že i dokumenty bez atributu `friends` jsou korektně zpracovány jako prázdné pole a nezpůsobí chybu pipeline. Výstup identifikuje uživatele s nejvyšší deklarovanou sociální konektivitou v rámci datasetu.

    db.users.aggregate(
      [
        {
          $project: {
            name: 1,
            friend_count: {
              $size: {
                $ifNull: ["$friends", []]
              }
            }
          }
        },
        {
          $sort: { friend_count: -1 }
        },
        {
          $limit: 10
        }
      ],
      { allowDiskUse: true }
    )

### Dotaz 16: Podniky s otevírací dobou alespoň pro 5 dní
Vnořený objekt `hours` je pomocí `$objectToArray` převeden na pole klíč–hodnota, takže nad ním lze aplikovat operátor `$size`. Výsledná metrika `days_open` reprezentuje počet dní v týdnu, pro které je u podniku evidována otevírací doba. Filtrem `days_open >= 5` jsou vybrány provozy s širší provozní dostupností, což je ukázka kvantifikace rozsahu nested dokumentu.

    db.businesses.aggregate(
      [
        {
          $project: {
            name: 1,
            city: 1,
            days_open: {
              $size: {
                $objectToArray: {
                  $ifNull: ["$hours", {}]
                }
              }
            }
          }
        },
        {
          $match: {
            days_open: { $gte: 5 }
          }
        },
        {
          $sort: { days_open: -1 }
        }
      ],
      { allowDiskUse: true }
    )

### Dotaz 17: Počet podniků podle hodnoty atributu WiFi
Dotaz analyzuje hodnotu vnořeného atributu `attributes.WiFi`, který je nejprve omezen pouze na dokumenty, kde skutečně existuje. Následná projekce a grupace vytváří distribuci jednotlivých variant dostupnosti Wi‑Fi, například bez připojení, s bezplatným nebo placeným přístupem. Jde o jednoduchou, ale praktickou ukázku agregace nad nested atributem uloženým v dokumentu.

    db.businesses.aggregate(
      [
        {
          $match: {
            "attributes.WiFi": { $exists: true }
          }
        },
        {
          $project: {
            wifi: "$attributes.WiFi"
          }
        },
        {
          $group: {
            _id: "$wifi",
            business_count: { $sum: 1 }
          }
        },
        {
          $sort: { business_count: -1 }
        }
      ],
      { allowDiskUse: true }
    )

### Dotaz 18: Počet elitních uživatelů podle roku
Pole `elite` je u každého uživatele rozbaleno na jednotlivé roky pomocí `$unwind`, čímž lze každé členství v elite programu zpracovat jako samostatný záznam. Po grupaci podle roku vznikne časová řada počtu elitních uživatelů. Dotaz je vhodný pro sledování vývoje aktivní či privilegované části komunity v čase.

    db.users.aggregate(
      [
        {
          $project: {
            elite: 1
          }
        },
        {
          $unwind: "$elite"
        },
        {
          $group: {
            _id: "$elite",
            elite_user_count: { $sum: 1 }
          }
        },
        {
          $sort: { _id: 1 }
        }
      ],
      { allowDiskUse: true }
    )


## 4. Textové a obsahové dotazy nad recenzemi

Následující dotazy pracují s textovým obsahem recenzí a převádějí nestrukturovaný text na analyzovatelné metriky. Kombinují filtrování regulárními výrazy, tokenizaci, měření délky textu a propojení textových charakteristik s hodnocením a reakcemi komunity.

### Dotaz 19: Recenze obsahující slovo „great“
První fáze využívá regex filtr `/great/i`, který provede case‑insensitive vyhledání zadaného tokenu v textu recenze. Po textovém filtrování následuje dvojice `$lookup` operací, které doplní identitu autora i název hodnoceného podniku, takže výsledný dataset propojuje obsahový filtr se strukturálními metadaty. Dotaz ukazuje, jak kombinovat textové vyhledání s dokumentovým joinem.

    db.reviews.aggregate(
      [
        {
          $match: {
            text: /great/i
          }
        },
        {
          $project: {
            review_id: 1,
            user_id: 1,
            business_id: 1,
            stars: 1,
            text: 1
          }
        },
        {
          $lookup: {
            from: "users",
            localField: "user_id",
            foreignField: "user_id",
            as: "user"
          }
        },
        {
          $lookup: {
            from: "businesses",
            localField: "business_id",
            foreignField: "business_id",
            as: "business"
          }
        },
        {
          $unwind: "$user"
        },
        {
          $unwind: "$business"
        },
        {
          $project: {
            _id: 0,
            review_id: 1,
            stars: 1,
            user_name: "$user.name",
            business_name: "$business.name",
            text: 1
          }
        },
        {
          $limit: 20
        }
      ],
      { allowDiskUse: true }
    )

### Dotaz 20: Nejčastější slova v pětihvězdičkových recenzích
Pipeline provádí základní tokenizaci textu recenzí nad pětihvězdičkovými záznamy pomocí převodu na lowercase a rozdělení podle mezery (`$split`). Po rozbalení tokenů přes `$unwind` jsou odfiltrována vybraná stop‑slova a následná grupace vytváří frekvenční distribuci nejčastějších výrazů. Jde o jednoduchou demonstraci text mining přístupu přímo v MongoDB bez externího NLP zpracování.

    db.reviews.aggregate(
      [
        {
          $match: {
            stars: 5,
            text: { $type: "string" }
          }
        },
        {
          $project: {
            words: {
              $split: [
                { $toLower: "$text" },
                " "
              ]
            }
          }
        },
        {
          $unwind: "$words"
        },
        {
          $match: {
            words: {
              $nin: ["", "the", "and", "a", "to", "of", "it", "is", "in"]
            }
          }
        },
        {
          $group: {
            _id: "$words",
            count: { $sum: 1 }
          }
        },
        {
          $sort: { count: -1 }
        },
        {
          $limit: 20
        }
      ],
      { allowDiskUse: true }
    )

### Dotaz 21: Průměrná délka recenze podle počtu hvězdiček
Pomocí `$strLenCP` je pro každou recenzi vypočtena délka textu v Unicode code pointech, což je bezpečnější než prosté bajtové měření u vícejazyčných textů. Agregace následně seskupí recenze podle počtu hvězdiček a pro každou skupinu vypočítá průměrnou délku i počet pozorování. Dotaz umožňuje zkoumat, zda se rozsah recenzního textu systematicky mění podle polarity hodnocení.

    db.reviews.aggregate(
      [
        {
          $project: {
            stars: 1,
            text_length: { $strLenCP: "$text" }
          }
        },
        {
          $group: {
            _id: "$stars",
            avg_text_length: { $avg: "$text_length" },
            review_count: { $sum: 1 }
          }
        },
        {
          $sort: { _id: 1 }
        }
      ],
      { allowDiskUse: true }
    )

### Dotaz 22: Podniky s větším počtem dlouhých recenzí a nízkým hodnocením
Dotaz nejprve odvodí délku recenze, poté omezuje dataset na textově rozsáhlé recenze (`>= 500` znaků) a agreguje je podle `business_id`. Druhá filtrační podmínka vyžaduje alespoň 10 takových recenzí, čímž eliminuje náhodné extrémy s malou četností. Po doplnění metadat z kolekce `businesses` a filtru na `business.stars <= 3.0` vzniká seznam podniků, které generují nadprůměrně mnoho dlouhých a současně negativněji hodnocených recenzí.

    db.reviews.aggregate(
      [
        {
          $project: {
            business_id: 1,
            text_length: { $strLenCP: "$text" }
          }
        },
        {
          $match: {
            text_length: { $gte: 500 }
          }
        },
        {
          $group: {
            _id: "$business_id",
            long_review_count: { $sum: 1 }
          }
        },
        {
          $match: {
            long_review_count: { $gte: 10 }
          }
        },
        {
          $lookup: {
            from: "businesses",
            localField: "_id",
            foreignField: "business_id",
            as: "business"
          }
        },
        {
          $unwind: "$business"
        },
        {
          $match: {
            "business.stars": { $lte: 3.0 }
          }
        },
        {
          $project: {
            _id: 0,
            business_name: "$business.name",
            city: "$business.city",
            stars: "$business.stars",
            long_review_count: 1
          }
        },
        {
          $sort: { long_review_count: -1 }
        }
      ],
      { allowDiskUse: true }
    )

### Dotaz 23: Vztah délky recenze a useful
Pipeline používá operátor `$bucket`, který rozděluje recenze do předem definovaných intervalů podle délky textu. Pro každý interval je vypočten průměr `useful` i počet záznamů, čímž vzniká agregovaný pohled na vztah mezi délkou obsahu a komunitním hodnocením užitečnosti. Tento přístup je vhodný pro segmentační analýzu bez nutnosti explicitního vypisování každé jednotlivé délky.

    db.reviews.aggregate(
      [
        {
          $project: {
            useful: 1,
            text_length: { $strLenCP: "$text" }
          }
        },
        {
          $bucket: {
            groupBy: "$text_length",
            boundaries: [0, 100, 300, 600, 1000, 5000],
            default: "1000+",
            output: {
              avg_useful: { $avg: "$useful" },
              count: { $sum: 1 }
            }
          }
        }
      ],
      { allowDiskUse: true }
    )

### Dotaz 24: Uživatelé píšící nejdelší recenze
Agregace probíhá na úrovni uživatele a z recenzí odvozuje průměrnou délku textu, průměrně udělované hodnocení a počet recenzí. Filtr `review_count >= 10` zajišťuje, že do rankingu vstupují pouze uživatelé s dostatečným objemem dat, nikoli jednorázové extrémy. Po doplnění jména uživatele je výstup seřazen podle `avg_text_length`, takže identifikuje autory s nejobsáhlejším stylem recenzování.

    db.reviews.aggregate(
      [
        {
          $project: {
            user_id: 1,
            stars: 1,
            text_length: { $strLenCP: "$text" }
          }
        },
        {
          $group: {
            _id: "$user_id",
            avg_text_length: { $avg: "$text_length" },
            avg_given_stars: { $avg: "$stars" },
            review_count: { $sum: 1 }
          }
        },
        {
          $match: {
            review_count: { $gte: 10 }
          }
        },
        {
          $lookup: {
            from: "users",
            localField: "_id",
            foreignField: "user_id",
            as: "user"
          }
        },
        {
          $unwind: "$user"
        },
        {
          $project: {
            _id: 0,
            user_name: "$user.name",
            avg_text_length: 1,
            avg_given_stars: 1,
            review_count: 1
          }
        },
        {
          $sort: { avg_text_length: -1 }
        },
        {
          $limit: 10
        }
      ],
      { allowDiskUse: true }
    )


## 5. Konzistence dat, kvalita dat a kontrolní dotazy

Poslední část je zaměřena na kontrolu kvality a konzistence dat napříč kolekcemi. Dotazy ověřují úplnost vybraných atributů, referenční vazby mezi entitami a soulad uložených agregačních polí se skutečně odvoditelnými hodnotami.

### Dotaz 25: Podniky s chybějící adresou, městem nebo PSČ
Jednoduchý `find` dotaz ověřuje úplnost základních adresních atributů v kolekci `businesses`. Podmínka `$or` kombinuje tři typy nekvalitních stavů — chybějící pole, `null` hodnotu a prázdný řetězec — pro atributy `address`, `city` a `postal_code`. Výsledek slouží jako rychlá kontrola datové úplnosti a může být použit jako vstup pro následné čištění dat.

    db.businesses.find(
      {
        $or: [
          { address: { $exists: false } },
          { address: null },
          { address: "" },
          { city: { $exists: false } },
          { city: null },
          { city: "" },
          { postal_code: { $exists: false } },
          { postal_code: null },
          { postal_code: "" }
        ]
      },
      {
        _id: 0,
        business_id: 1,
        name: 1,
        address: 1,
        city: 1,
        postal_code: 1
      }
    ).limit(20)

### Dotaz 26: Uživatelé, u nichž nesedí review_count
Dotaz konfrontuje denormalizované pole `review_count` uložené v kolekci `users` se skutečným počtem souvisejících recenzí nalezených přes `$lookup` v kolekci `reviews`. Po výpočtu velikosti napojeného pole je odvozena diference `diff`, která představuje velikost nesouladu mezi uloženým a reálně dopočteným stavem. Tento typ kontroly je zásadní po importu, migraci nebo dávkové transformaci dat.

    db.users.aggregate(
      [
        {
          $project: {
            user_id: 1,
            name: 1,
            review_count: 1
          }
        },
        {
          $lookup: {
            from: "reviews",
            localField: "user_id",
            foreignField: "user_id",
            as: "reviews"
          }
        },
        {
          $project: {
            name: 1,
            stored_review_count: "$review_count",
            real_review_count: { $size: "$reviews" }
          }
        },
        {
          $project: {
            name: 1,
            stored_review_count: 1,
            real_review_count: 1,
            diff: { $subtract: ["$stored_review_count", "$real_review_count"] }
          }
        },
        {
          $match: {
            diff: { $ne: 0 }
          }
        },
        {
          $sort: { diff: -1 }
        }
      ],
      { allowDiskUse: true }
    )

### Dotaz 27: Recenze odkazující na neexistujícího uživatele nebo podnik
Pipeline provádí dvojitý referenční audit nad kolekcí `reviews`: pro každý záznam ověřuje existenci odpovídajícího uživatele i podniku. Záznamy, u nichž jedno z napojených polí zůstane po `$lookup` prázdné, jsou vyhodnoceny jako referenčně nekonzistentní a vráceny ve výsledku. Závěrečná projekce doplňuje explicitní booleovské indikátory `missing_user` a `missing_business`, což usnadňuje následnou diagnostiku problému.

    db.reviews.aggregate(
      [
        {
          $project: {
            review_id: 1,
            user_id: 1,
            business_id: 1
          }
        },
        {
          $lookup: {
            from: "users",
            localField: "user_id",
            foreignField: "user_id",
            as: "user"
          }
        },
        {
          $lookup: {
            from: "businesses",
            localField: "business_id",
            foreignField: "business_id",
            as: "business"
          }
        },
        {
          $match: {
            $or: [
              { "user.0": { $exists: false } },
              { "business.0": { $exists: false } }
            ]
          }
        },
        {
          $project: {
            _id: 0,
            review_id: 1,
            user_id: 1,
            business_id: 1,
            missing_user: {
              $cond: [{ $eq: [{ $size: "$user" }, 0] }, true, false]
            },
            missing_business: {
              $cond: [{ $eq: [{ $size: "$business" }, 0] }, true, false]
            }
          }
        }
      ],
      { allowDiskUse: true }
    )

### Dotaz 28: Podniky, u nichž nesedí review_count
Stejný validační princip jako u uživatelů je zde aplikován na podniky: uložené pole `review_count` v kolekci `businesses` je porovnáváno se skutečnou velikostí joinovaného pole recenzí. Agregace následně vypočítá rozdíl mezi oběma hodnotami a vrátí pouze nekonzistentní záznamy. Dotaz je vhodný pro ověření správnosti denormalizovaných souhrnných atributů po importu nebo reindexaci dat.

    db.businesses.aggregate(
      [
        {
          $project: {
            business_id: 1,
            name: 1,
            review_count: 1
          }
        },
        {
          $lookup: {
            from: "reviews",
            localField: "business_id",
            foreignField: "business_id",
            as: "reviews"
          }
        },
        {
          $project: {
            name: 1,
            stored_review_count: "$review_count",
            real_review_count: { $size: "$reviews" }
          }
        },
        {
          $project: {
            name: 1,
            stored_review_count: 1,
            real_review_count: 1,
            diff: { $subtract: ["$stored_review_count", "$real_review_count"] }
          }
        },
        {
          $match: {
            diff: { $ne: 0 }
          }
        },
        {
          $sort: { diff: -1 }
        }
      ],
      { allowDiskUse: true }
    )

### Dotaz 29: Zavřené podniky, které mají vůbec evidované recenze
Dotaz vybírá podniky označené jako uzavřené (`is_open: 0`) a následně k nim přes `$lookup` dohledává navázané recenze. Pomocí `$size` je vypočten skutečný počet recenzí odvozený ze zdrojové kolekce `reviews`, přičemž ve výsledku zůstávají pouze podniky s nenulovou historií recenzí. Jde o kontrolní analytický dotaz, který kombinuje provozní stav podniku s jeho historickou stopou v recenzním subsystému.

    db.businesses.aggregate(
      [
        {
          $match: {
            is_open: 0
          }
        },
        {
          $project: {
            business_id: 1,
            name: 1,
            city: 1,
            state: 1
          }
        },
        {
          $lookup: {
            from: "reviews",
            localField: "business_id",
            foreignField: "business_id",
            as: "reviews"
          }
        },
        {
          $project: {
            name: 1,
            city: 1,
            state: 1,
            review_count_from_reviews: { $size: "$reviews" }
          }
        },
        {
          $match: {
            review_count_from_reviews: { $gt: 0 }
          }
        },
        {
          $sort: {
            review_count_from_reviews: -1
          }
        },
        {
          $limit: 20
        }
      ],
      { allowDiskUse: true }
    )
    Co ten dotaz dělá
    •	vybere jen podniky, které jsou označené jako zavřené (is_open: 0) 
    •	dohledá k nim recenze 
    •	spočítá, kolik recenzí k nim existuje 
    •	vrátí jen ty zavřené podniky, které nějaké recenze mají 
    Tohle je:
    •	pořád smysluplný kontrolní dotaz 
    •	pořád netriviální 
    •	a mnohem pravděpodobněji ti vrátí výstup 
    ________________________________________

### Dotaz 30: Podniky generující největší objem recenzí
Pipeline agreguje kolekci `reviews` podle `business_id` a pro každý podnik počítá objem recenzí i průměrné hodnocení odvozené přímo z recenzních dat. Seřazení a omezení na top 20 probíhá ještě před připojením kolekce `businesses`, což redukuje náklady následného joinu a je z hlediska výkonu výhodnější. Výsledkem je seznam podniků s nejvyšší recenzní exponovaností doplněný o identifikační a lokalizační metadata.

    db.reviews.aggregate(
      [
        {
          $project: {
            business_id: 1,
            stars: 1
          }
        },
        {
          $group: {
            _id: "$business_id",
            review_count: { $sum: 1 },
            avg_stars_from_reviews: { $avg: "$stars" }
          }
        },
        {
          $sort: { review_count: -1 }
        },
        {
          $limit: 20
        },
        {
          $lookup: {
            from: "businesses",
            localField: "_id",
            foreignField: "business_id",
            as: "business"
          }
        },
        {
          $unwind: "$business"
        },
        {
          $project: {
            _id: 0,
            business_id: "$_id",
            business_name: "$business.name",
            city: "$business.city",
            state: "$business.state",
            review_count: 1,
            avg_stars_from_reviews: 1
          }
        }
      ],
      { allowDiskUse: true }
    )
