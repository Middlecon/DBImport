import { useEffect } from 'react'

export function useCustomSelection(
  ref: React.RefObject<HTMLElement>,
  isDropdownOpen: boolean
) {
  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      // Checks if Cmd + A (Mac) or Ctrl + A (Windows) is pressed when the dropdown is open
      if (
        isDropdownOpen &&
        (event.metaKey || event.ctrlKey) &&
        event.key === 'a'
      ) {
        event.preventDefault()

        if (ref.current) {
          const selection = window.getSelection()
          const range = document.createRange()

          // Selects all the text inside the dropdown
          range.selectNodeContents(ref.current)
          selection?.removeAllRanges()
          selection?.addRange(range)
        }
      }
    }

    document.addEventListener('keydown', handleKeyDown)

    // Cleanup listener on unmount
    return () => {
      document.removeEventListener('keydown', handleKeyDown)
    }
  }, [isDropdownOpen, ref])
}
