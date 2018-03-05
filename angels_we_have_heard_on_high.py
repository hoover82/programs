# A Christmas carol in Python

lyrics = "Angels we have heard on high,\n"
lyrics += 'Sweetly singing o\'er the plains.\n'
lyrics += "And the mountains in reply,\n"
lyrics += "Echoing their joyous strains!\n"

for n in range (2):
    for i in range (3):
        if i == 0:
            lyrics += "Glor-"

        else:
            lyrics += "orrrrrr-"

        for j in range (4):
             lyrics += "o-"


    lyrics += "oria!\n"

    lyrics += "In excelsis deo!"

    if n == 0:
        lyrics += "\n"

print ( lyrics )
