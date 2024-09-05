import './Home.scss'

function Home() {
  const authToken: string | null = sessionStorage.getItem('DBI_auth_token')
  console.log('DBI_auth_token', authToken)

  return (
    <div className="home-root">
      <div className="home-text-block">
        <h1>Welcome to DBImport!</h1>
        <p>Select action in the menu to the left.</p>
      </div>
    </div>
  )
}

export default Home
